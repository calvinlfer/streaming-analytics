package com.experiments.calvin

import akka.NotUsed
import akka.kafka.ConsumerMessage
import akka.stream.FlowShape
import akka.stream.scaladsl._
import com.experiments.calvin.Main.countByYMDH
import com.experiments.calvin.models._
import com.experiments.calvin.repositories.EventCountByYMDH
import io.circe._
import io.circe.parser._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3

trait Streams {
  val settings: Settings

  def filterBadMessages[A](implicit decoder: Decoder[A]): Flow[KafkaEnvelope[String], KafkaEnvelope[A], NotUsed] = {
    val cleaner: Flow[KafkaEnvelope[Either[Error, A]], KafkaEnvelope[A], NotUsed] = Flow.fromGraph(GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[KafkaEnvelope[Either[io.circe.Error, A]]](2))

        // Bad Messages (commit them)
        // TODO: should consider pushing them to an error topic
        broadcast
          .out(0)
          .filter(_.payload.isLeft)
          .map(_.offset)
          .map(_.commitScaladsl())
          .to(Sink.ignore)

        // Good messages
        val output = broadcast.out(1).filter(_.payload.isRight).map(ke => ke.copy(payload = ke.payload.right.get))

        FlowShape(broadcast.in, output.outlet)
    })
    Flow[KafkaEnvelope[String]]
      .map { case KafkaEnvelope(payloadStr, offset) => KafkaEnvelope(decode[A](payloadStr), offset) }
      .via(cleaner)
  }

  def persistEventCounts(eventsRepo: EventCountByYMDH)(
      implicit ec: ExecutionContext
  ): Flow[KafkaEnvelope[AnalyticsEvent], immutable.Seq[ConsumerMessage.CommittableOffset], NotUsed] = {

    def saveEvents(repo: EventCountByYMDH, eventType: String)(
        envs: immutable.Seq[KafkaEnvelope[AnalyticsEvent]]
    ): Future[immutable.Seq[ConsumerMessage.CommittableOffset]] = {
      val analytics: immutable.Seq[AnalyticsEvent]                  = envs.map(_.payload)
      val offsets: immutable.Seq[ConsumerMessage.CommittableOffset] = envs.map(_.offset)

      Future
        .sequence {
          countByYMDH(analytics).map {
            case (YMDH(y, m, d, h), addCount) =>
              repo.persist(y, m, d, h, eventType, addCount)
          }
        }
        .map(_ => offsets)
    }

    def eventPartitioner(e: KafkaEnvelope[AnalyticsEvent]): Int = if (e.payload.event == "click") 0 else 1

    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partitioner = builder.add(
        Partition[KafkaEnvelope[AnalyticsEvent]](outputPorts = 2, partitioner = eventPartitioner)
      )

      val merger =
        builder.add(Merge[immutable.Seq[ConsumerMessage.CommittableOffset]](inputPorts = 2, eagerComplete = false))

      val batchSettings = settings.batch

      // click events
      partitioner
        .out(0)
        .groupedWithin(batchSettings.maxElements, batchSettings.maxDuration)
        .mapAsync(batchSettings.updateConcurrency)(saveEvents(eventsRepo, "click")) ~> merger.in(0)

      // impression events
      partitioner
        .out(1)
        .groupedWithin(batchSettings.maxElements, batchSettings.maxDuration)
        .mapAsync(batchSettings.updateConcurrency)(saveEvents(eventsRepo, "impression")) ~> merger.in(1)

      FlowShape(partitioner.in, merger.out)
    })
  }
}
