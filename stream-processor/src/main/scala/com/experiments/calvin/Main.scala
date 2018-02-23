package com.experiments.calvin

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.experiments.calvin.models.{AnalyticsEvent, KafkaEnvelope, UserId}
import com.experiments.calvin.repositories.AppDatabase
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.parser._
import io.circe.syntax._
import net.agkn.hll.HLL
import com.outworkers.phantom.dsl._
import net.agkn.hll.serialization.SerializationUtil

import scala.concurrent.Future
import scala.concurrent.duration._

object Main extends App with Aggregation {
  implicit val system: ActorSystem    = ActorSystem("kafka-event-consumer")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  val settings: Settings              = Settings(system)
  val appDatabase                     = AppDatabase(settings)
  appDatabase.create(30.seconds)
  val uniqueUsersRepo = appDatabase.uniqueUsersByYMDH

  val consumerSettings = ConsumerSettings(
    system = system,
    keyDeserializer = new StringDeserializer,
    valueDeserializer = new StringDeserializer
  ).withBootstrapServers(settings.kafka.uris)
    .withGroupId(settings.kafka.consumerGroupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // consume from the earliest offset when no initial offset is present

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(settings.kafka.topic))
    .map(
      commitableMsg =>
        KafkaEnvelope(payload = decode[AnalyticsEvent](commitableMsg.record.value()),
                      offset = commitableMsg.committableOffset)
    )
    .filter(_.payload.isRight) // TODO: bad messages should go in another topic and be removed
    .map(env => env.map(_.right.get))
    .groupedWithin(10000, 30.seconds)
    .mapAsync(1) { envs =>
      val analytics                        = envs.map(_.payload)
      val offsets                          = envs.map(_.offset)
      val result: Seq[(YMDH, Seq[UserId])] = uniqueUserIdByYMDH(analytics)

      val currentHLLMappings: Map[YMDH, HLL] = userEstimatesByYMDH(result).toMap
      val dbHLLMappings: Future[Map[Main.YMDH, HLL]] =
        Future
          .sequence(
            currentHLLMappings.keys
              .map(ymdh => uniqueUsersRepo.find(ymdh.year, ymdh.month, ymdh.day, ymdh.hour))
          )
          .map(_.flatten)
          .map(
            seq =>
              seq.foldLeft(Map.empty[YMDH, HLL]) { (acc, next) =>
                val hllBytes = next.hllBytes.array()
                val hll      = HLL.fromBytes(hllBytes)
                acc + (YMDH(next.year, next.month, next.day, next.hour) -> hll)
            }
          )

      val futMergedHLLMappings: Future[Map[YMDH, HLL]] = dbHLLMappings.map { dbHLLs =>
        mergeHLLMappings(currentHLLMappings, dbHLLs)
      }

      val persistResults = futMergedHLLMappings.map { mergedHLLMappings =>
        mergedHLLMappings.map {
          case (YMDH(y, m, d, h), hll) =>
            val hllBytes = ByteBuffer.wrap(hll.toBytes(SerializationUtil.VERSION_ONE))
            println(s"For Year $y, Month $m, Day $d, Hour $h, ${hll.cardinality()} unique users came through")
            uniqueUsersRepo.persist(y, m, d, h, hllBytes)
        }
      }
      persistResults.map(_ => offsets)
    }
    .mapConcat(identity)
    .batch(max = 20, seedOffset => CommittableOffsetBatch.empty.updated(seedOffset))((acc, next) => acc.updated(next))
    .mapAsync(1)(_.commitScaladsl())
    .to(Sink.ignore)
    .run()
}
