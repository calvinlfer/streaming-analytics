package com.experiments.calvin.web

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import scala.util.Try

trait Web {
  sealed trait Event
  case object Click      extends Event
  case object Impression extends Event

  def extractEvent(action: Event => Route): Route =
    parameter('event) { rawEvent =>
      val tryEvent: Try[Event] = Try {
        rawEvent match {
          case "click"      => Click
          case "impression" => Impression
        }
      }

      tryEvent.fold(
        _ => complete(BadRequest, "event must be click or impression"),
        event => action(event)
      )
    }

  def extractTimestamp(action: Instant => Route): Route =
    parameter('timestamp.as[Long]) { rawTimestamp =>
      val tryInstant = Try(Instant.ofEpochMilli(rawTimestamp))
      tryInstant.fold(
        throwable => complete(BadRequest, throwable.getMessage),
        validInstant => action(validInstant)
      )
    }

  val routes: Route =
    pathPrefix("analytics") {
      post {
        extractEvent { event =>
          extractTimestamp { instant =>
            complete(s"POST: I got $event and $instant")
          }
        }
      } ~ get {
        extractTimestamp { instant =>
          complete(s"GET: I got $instant")
        }
      }
    }
}
