package com.experiments.calvin.web

import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.experiments.calvin.models.{Event, UserInteraction}
import com.experiments.calvin.models.Event._
import com.experiments.calvin.services.{Journal, UserAnalytics}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe._
import io.circe.generic.auto._

import scala.util.Try

trait Web {
  val journal: Journal
  val analytics: UserAnalytics

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

  def extractTimestamp(action: ZonedDateTime => Route): Route =
    parameter('timestamp.as[Long]) { rawTimestamp =>
      val tryInstant = Try(Instant.ofEpochMilli(rawTimestamp))
      tryInstant.fold(
        throwable => complete(BadRequest, throwable.getMessage),
        validInstant => action(validInstant.atZone(ZoneId.of("UTC")))
      )
    }

  val routes: Route =
    pathPrefix("analytics") {
      post {
        parameter("userId") { userId =>
          extractEvent { event =>
            extractTimestamp { instant =>
              onComplete(journal.persist(UserInteraction(instant, event, userId))) {
                case util.Success(_) => complete(OK)
                case util.Failure(_) => complete(InternalServerError)
              }
            }
          }
        }
      } ~ get {
        extractTimestamp { zdt =>
          onComplete(analytics.dataForTimestamp(zdt)) {
            case util.Success(data) => complete(data)
            case util.Failure(_)    => complete(InternalServerError)
          }
        }
      }
    }
}
