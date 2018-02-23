package com.experiments.calvin

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.experiments.calvin.models.{AnalyticsEvent, KafkaEnvelope, UserId}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.parser._
import io.circe.syntax._
import net.agkn.hll.HLL

import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3

object Main extends App with Aggregation {
  implicit val system: ActorSystem    = ActorSystem("kafka-event-consumer")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  val settings: Settings              = Settings(system)

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
    .map { envs =>
      val analytics                        = envs.map(_.payload)
      val offsets                          = envs.map(_.offset)
      val result: Seq[(YMDH, Seq[UserId])] = uniqueUserIdByYMDH(analytics)
      val hllMappings: Seq[(YMDH, HLL)] = result.map {
        case (ymdh: YMDH, userIds: Seq[UserId]) =>
          val log2m    = 13
          val regWidth = 5
          val hll      = new HLL(log2m, regWidth)
          userIds
            .map(MurmurHash3.stringHash(_).toLong)
            .foreach(hll.addRaw)
          ymdh -> hll
      }
      hllMappings.foreach {
        case (ymdh, hll) =>
          println(s"YMDH: $ymdh, estimate: ${hll.cardinality()}")
      }
      offsets
    }
    .mapConcat(identity)
    .batch(max = 20, seedOffset => CommittableOffsetBatch.empty.updated(seedOffset))((acc, next) => acc.updated(next))
    .mapAsync(1)(_.commitScaladsl())
    .to(Sink.ignore)
    .run()
}
