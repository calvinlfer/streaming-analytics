package com.experiments.calvin

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.kafka.ProducerSettings
import akka.stream.ActorMaterializer
import com.experiments.calvin.services.{Journal, KafkaJournal}
import com.experiments.calvin.web.Web
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main extends App with Web {
  implicit val system: ActorSystem    = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext   = system.dispatcher
  val settings: Settings              = Settings(system)

  val kafkaProducerSettings: ProducerSettings[String, String] =
    ProducerSettings(
      system = system,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer
    ).withBootstrapServers(settings.kafka.uris)

  override val journal: Journal = new KafkaJournal(settings, mat, kafkaProducerSettings)(ec)

  Http().bindAndHandle(routes, "0.0.0.0", 9001).onComplete {
    case Success(binding) =>
      println(s"Server online at http://${binding.localAddress.getHostName}:${binding.localAddress.getPort}")

    case Failure(ex) =>
      println(s"Failed to start server, shutting down actor system. Exception is: ${ex.getCause}: ${ex.getMessage}")
      system.terminate()
  }
}
