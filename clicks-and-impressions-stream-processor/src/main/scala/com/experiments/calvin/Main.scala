package com.experiments.calvin

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl._
import com.experiments.calvin.models.{AnalyticsEvent, KafkaEnvelope}
import com.experiments.calvin.repositories.AppDatabase
import com.outworkers.phantom.dsl._
import io.circe.generic.auto._
import io.circe.java8.time._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

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

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(settings.kafka.topic))
    .map(kafkaMsg => KafkaEnvelope(kafkaMsg.record.value(), kafkaMsg.committableOffset))
    .via(filterBadMessages[AnalyticsEvent])
    .via(persistEventCounts(eventsRepo))
    .mapConcat(identity)
    .batch(max = 20, seedOffset => CommittableOffsetBatch.empty.updated(seedOffset))((acc, next) => acc.updated(next))
    .mapAsync(1)(_.commitScaladsl())
    .to(Sink.ignore)
    .run()
}
