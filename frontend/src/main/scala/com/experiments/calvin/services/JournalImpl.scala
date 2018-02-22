package com.experiments.calvin.services
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.experiments.calvin.models.UserInteraction
import io.circe._
import io.circe.java8.time._
import io.circe.parser._

import scala.concurrent.{ExecutionContext, Future}

class JournalImpl(kafkaJournal: ActorRef)(implicit timeout: Timeout, ec: ExecutionContext) extends Journal {
  override def persist(ui: UserInteraction): Future[Unit] =
    (kafkaJournal ? ui).map(_ => ())
}
