package com.experiments.calvin

import java.nio.ByteBuffer

import akka.NotUsed
import akka.kafka.ConsumerMessage
import akka.stream.FlowShape
import akka.stream.scaladsl._
import com.experiments.calvin.Main.{mergeHLLMappings, uniqueUserIdByYMDH, userEstimatesByYMDH}
import com.experiments.calvin.models._
import com.experiments.calvin.repositories.{RepoUniqueUsers, UniqueUsersByYMDH}
import net.agkn.hll.HLL
import net.agkn.hll.serialization.SerializationUtil
import io.circe._
import io.circe.parser._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

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

  def uniqueUsersByYMDHFlow(uniqueUsersRepo: UniqueUsersByYMDH)(
      implicit ec: ExecutionContext
  ): Flow[immutable.Seq[KafkaEnvelope[AnalyticsEvent]], immutable.Seq[ConsumerMessage.CommittableOffset], NotUsed] = {
    def dBReprToInMemoryRepr(seq: Iterable[RepoUniqueUsers]): Map[YMDH, HLL] =
      seq.foldLeft(Map.empty[YMDH, HLL]) { (acc, next) =>
        val hllBytes = next.hllBytes.array()
        val hll      = HLL.fromBytes(hllBytes)
        acc + (YMDH(next.year, next.month, next.day, next.hour) -> hll)
      }

    Flow[immutable.Seq[KafkaEnvelope[AnalyticsEvent]]].mapAsync(settings.batch.updateConcurrency) {
      envs: immutable.Seq[KafkaEnvelope[AnalyticsEvent]] =>
        val analytics: immutable.Seq[AnalyticsEvent]                  = envs.map(_.payload)
        val offsets: immutable.Seq[ConsumerMessage.CommittableOffset] = envs.map(_.offset)
        val result: Seq[(YMDH, Seq[UserId])]                          = uniqueUserIdByYMDH(analytics)

        val currentHLLMappings: Map[YMDH, HLL] = userEstimatesByYMDH(result).toMap
        val dbHLLMappings: Future[Map[YMDH, HLL]] =
          Future
            .sequence(
              currentHLLMappings.keys
                .map(ymdh => uniqueUsersRepo.find(ymdh.year, ymdh.month, ymdh.day, ymdh.hour))
            )
            .map(_.flatten)
            .map(dBReprToInMemoryRepr)

        val futMergedHLLMappings: Future[Map[YMDH, HLL]] = dbHLLMappings.map { dbHLLs =>
          mergeHLLMappings(currentHLLMappings, dbHLLs)
        }

        val persistResults = futMergedHLLMappings.map { mergedHLLMappings =>
          mergedHLLMappings.map {
            case (YMDH(y, m, d, h), hll) =>
              val hllBytes = ByteBuffer.wrap(hll.toBytes(SerializationUtil.VERSION_ONE))
              uniqueUsersRepo.persist(y, m, d, h, hllBytes)
          }
        }
        persistResults.map(_ => offsets)
    }
  }
}
