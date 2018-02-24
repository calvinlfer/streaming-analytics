package com.experiments.calvin

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl._
import com.experiments.calvin.models.{AnalyticsEvent, KafkaEnvelope, YMDH}
import com.experiments.calvin.repositories.{AppDatabase, EventCountByYMDH}
import com.outworkers.phantom.dsl._
import io.circe.generic.auto._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import io.circe.java8.time._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

object Main extends App with Aggregation with Streams {
  implicit val system: ActorSystem    = ActorSystem("kafka-event-consumer")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  val settings: Settings              = Settings(system)
  val appDatabase                     = AppDatabase(settings)
  appDatabase.create(30.seconds)
  val eventsRepo = appDatabase.eventCountByYMDH

  val consumerSettings = ConsumerSettings(
    system = system,
    keyDeserializer = new StringDeserializer,
    valueDeserializer = new StringDeserializer
  ).withBootstrapServers(settings.kafka.uris)
    .withGroupId(settings.kafka.consumerGroupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // consume from the earliest offset when no initial offset is present

  def saveEvents(repo: EventCountByYMDH, eventType: String)(
      envs: immutable.Seq[KafkaEnvelope[AnalyticsEvent]]
  ): Future[immutable.Seq[ConsumerMessage.CommittableOffset]] = {
    val analytics: immutable.Seq[AnalyticsEvent]                  = envs.map(_.payload)
    val offsets: immutable.Seq[ConsumerMessage.CommittableOffset] = envs.map(_.offset)

    Future
      .sequence {
        countByYMDH(analytics).map {
          case (YMDH(y, m, d, h), addCount) =>
            repo.persist(y, m, d, h, "click", addCount)
        }
      }
      .map(_ => offsets)
  }

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(settings.kafka.topic))
    .map(kafkaMsg => KafkaEnvelope(kafkaMsg.record.value(), kafkaMsg.committableOffset))
    .via(filterBadMessages[AnalyticsEvent])
    .via(Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partitioner = builder.add(
        Partition[KafkaEnvelope[AnalyticsEvent]](outputPorts = 2,
                                                 partitioner = a =>
                                                   if (a.payload.event == "click") 0
                                                   else 1)
      )

      val merger =
        builder.add(Merge[immutable.Seq[ConsumerMessage.CommittableOffset]](inputPorts = 2, eagerComplete = false))

      // click events
      partitioner.out(0).groupedWithin(10000, 15.seconds).mapAsync(1)(saveEvents(eventsRepo, "click")) ~> merger.in(0)

      // impression events
      partitioner.out(1).groupedWithin(10000, 15.seconds).mapAsync(1)(saveEvents(eventsRepo, "impression")) ~> merger
        .in(1)

      FlowShape(partitioner.in, merger.out)
    }))
    .mapConcat(identity)
    .batch(max = 20, seedOffset => CommittableOffsetBatch.empty.updated(seedOffset))((acc, next) => acc.updated(next))
    .mapAsync(1)(_.commitScaladsl())
    .to(Sink.ignore)
    .run()
}
