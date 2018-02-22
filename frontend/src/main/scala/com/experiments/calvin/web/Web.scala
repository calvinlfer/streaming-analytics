package com.experiments.calvin.web

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.experiments.calvin.models.{Event, UserInteraction}
import com.experiments.calvin.models.Event._
import com.experiments.calvin.services.Journal

import scala.util.Try

trait Web {
  val journal: Journal

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
            onComplete(journal.persist(UserInteraction(instant, event))) {
              case util.Success(_) => complete(OK)
              case util.Failure(_) => complete(InternalServerError)
            }
          }
        }
      } ~ get {
        extractTimestamp { instant =>
          complete(s"GET: I got $instant")
        }
      }
    }
}
