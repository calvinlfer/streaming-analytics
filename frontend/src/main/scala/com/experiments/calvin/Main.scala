package com.experiments.calvin

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.kafka.ProducerSettings
import akka.pattern.BackoffSupervisor
import akka.routing.{DefaultResizer, SmallestMailboxPool}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.experiments.calvin.actors.KafkaJournal
import com.experiments.calvin.repositories.AppDatabase
import com.experiments.calvin.services.{Journal, JournalImpl, UserAnalytics, UserAnalyticsImpl}
import com.experiments.calvin.web.Web
import com.outworkers.phantom.dsl._
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main extends App with Web {
  implicit val system: ActorSystem    = ActorSystem("frontend")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext   = system.dispatcher
  val settings: Settings              = Settings(system)
  implicit val timeout: Timeout       = Timeout(settings.kafka.service.timeout)
  val appDatabase                     = AppDatabase(settings)
  appDatabase.create(30.seconds)

  val uniqueUsersRepo = appDatabase.uniqueUsersByYMDH

  val kafkaProducerSettings: ProducerSettings[String, String] =
    ProducerSettings(
      system = system,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer
    ).withBootstrapServers(settings.kafka.uris)

  val kafkaRef: ActorRef = {
    val actorSettings = settings.kafka.actor
    val props = BackoffSupervisor.props(
      SmallestMailboxPool(
        nrOfInstances = actorSettings.minInstances,
        resizer = Some(DefaultResizer(actorSettings.minInstances, actorSettings.maxInstances))
      ).props(KafkaJournal.props(settings, kafkaProducerSettings)),
      childName = "journal",
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
    system.actorOf(props, "kafka-router")
  }

  override val journal: Journal = new JournalImpl(kafkaRef)
  override val analytics: UserAnalytics = new UserAnalyticsImpl(uniqueUsersRepo)

  Http().bindAndHandle(routes, "0.0.0.0", 9001).onComplete {
    case Success(binding) =>
      println(s"Server online at http://${binding.localAddress.getHostName}:${binding.localAddress.getPort}")

    case Failure(ex) =>
      println(s"Failed to start server, shutting down actor system. Exception is: ${ex.getCause}: ${ex.getMessage}")
      system.terminate()
  }
}
