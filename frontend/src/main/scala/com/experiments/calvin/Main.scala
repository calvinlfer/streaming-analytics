package com.experiments.calvin

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.experiments.calvin.web.Web

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main extends App with Web {
  implicit val system: ActorSystem    = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext   = system.dispatcher

  Http().bindAndHandle(routes, "0.0.0.0", 9001).onComplete {
    case Success(binding) =>
      println(s"Server online at http://${binding.localAddress.getHostName}:${binding.localAddress.getPort}")

    case Failure(ex) =>
      println(s"Failed to start server, shutting down actor system. Exception is: ${ex.getCause}: ${ex.getMessage}")
      system.terminate()
  }
}
