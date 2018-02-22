package com.experiments.calvin.actors

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.kafka.scaladsl._
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl._
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, KillSwitch, KillSwitches, OverflowStrategy}
import com.experiments.calvin.Settings
import com.experiments.calvin.actors.KafkaJournal._
import com.experiments.calvin.models.UserInteraction
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class KafkaJournal(settings: Settings, producerSettings: ProducerSettings[String, String])
    extends Actor
    with ActorLogging {
  implicit val materializer: ActorMaterializer = ActorMaterializer()(context.system)
  implicit val ec: ExecutionContext            = context.dispatcher

  // WARNING: mutable state
  var optKillSwitch: Option[KillSwitch] = None

  override def preStart(): Unit = {
    log.info("Starting Event Consumer")
    self ! Start
  }

  override def postStop(): Unit = {
    // Shut the stream down if the Actor has started it
    optKillSwitch.foreach(_.shutdown())
  }

  def initial: Receive = {
    case Start =>
      val (journalActor, killSwitch, streamStatus) = Source
        .actorRef(bufferSize = settings.kafka.bufferSize, OverflowStrategy.dropHead)
        .map(
          (e: UserInteraction) =>
            ProducerMessage.Message(
              record = new ProducerRecord[String, String](
                settings.kafka.topic,
                kafkaTopicPartitioner(settings.kafka.partitionSize, e),
                e.timestamp.toEpochMilli.toString,
                e.asJson.noSpaces
              ),
              passThrough = e
          )
        )
        .via(Producer.flow(producerSettings))
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue { case ((actor, ks), comp) => (actor, ks, comp) }
        .run()

      streamStatus pipeTo self
      optKillSwitch = Some(killSwitch)
      context become operational(journalActor)
  }

  def operational(journal: ActorRef): Receive = {
    case u: UserInteraction =>
      journal ! u
      sender() ! Ack

    case Status.Failure(ex) =>
      log.error(ex, "The Kafka stream completed abnormally")
      throw ex

    case Done =>
      throw new Exception("The Kafka stream completed when it should not have") with NoStackTrace
  }

  override def receive: Receive = initial
}

object KafkaJournal {
  def props(settings: Settings, kafkaProducerSettings: ProducerSettings[String, String]): Props =
    Props(new KafkaJournal(settings, kafkaProducerSettings))

  private def kafkaTopicPartitioner(partitionSize: Int, e: UserInteraction): Int =
    e.timestamp.hashCode % partitionSize

  private sealed trait InternalMessage
  private case object Start extends InternalMessage

  sealed trait Message
  case object Ack extends Message
}
