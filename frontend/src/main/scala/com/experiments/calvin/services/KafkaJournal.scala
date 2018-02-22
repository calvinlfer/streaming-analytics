package com.experiments.calvin.services
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl._
import com.experiments.calvin.Settings
import com.experiments.calvin.models.UserInteraction
import org.apache.kafka.clients.producer.ProducerRecord
import KafkaJournal._
import akka.kafka.scaladsl._
import io.circe._
import io.circe.java8.time._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

class KafkaJournal(settings: Settings, mat: ActorMaterializer, kafkaProducerSettings: ProducerSettings[String, String])(
    implicit ec: ExecutionContext
) extends Journal {
  // TODO: this should really be inside an Actor for resilience
  val queue: SourceQueue[UserInteraction] = Source
    .queue[UserInteraction](settings.kafka.bufferSize, OverflowStrategy.dropHead)
    .map(
      (ui: UserInteraction) =>
        ProducerMessage.Message(
          record = new ProducerRecord[String, String](
            settings.kafka.topic,
            kafkaTopicPartitioner(settings.kafka.partitionSize, ui),
            ui.timestamp.getEpochSecond.toString,
            ui.asJson.noSpaces
          ),
          passThrough = ui
      )
    )
    .via(Producer.flow(kafkaProducerSettings))
    .to(Sink.ignore)
    .run()(mat)

  override def persist(ui: UserInteraction): Future[Unit] = {
    import QueueOfferResult._
    queue
      .offer(ui)
      .flatMap {
        case Enqueued =>
          Future.successful(())

        case Dropped =>
          Future.failed(new Exception("Elements being dropped, Kafka cannot handle load") with NoStackTrace)

        case Failure(cause) =>
          Future.failed(cause)

        case QueueClosed =>
          Future.failed(new Exception("Connection to Kafka closed") with NoStackTrace)
      }
  }
}

object KafkaJournal {
  private def kafkaTopicPartitioner(partitionSize: Int, e: UserInteraction): Int =
    e.timestamp.hashCode % partitionSize
}
