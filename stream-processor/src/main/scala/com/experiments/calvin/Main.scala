package com.experiments.calvin

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.experiments.calvin.models.{AnalyticsEvent, KafkaEnvelope}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.parser._
import io.circe.syntax._

object Main extends App {
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
    .map { env =>
      println(env.payload)
      env.offset
    }
    .batch(max = 20, seedOffset => CommittableOffsetBatch.empty.updated(seedOffset))((acc, next) => acc.updated(next))
    .mapAsync(1)(_.commitScaladsl())
    .to(Sink.ignore)
    .run()
}
