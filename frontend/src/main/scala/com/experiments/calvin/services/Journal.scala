package com.experiments.calvin.services

import com.experiments.calvin.models.UserInteraction

import scala.concurrent.Future

trait Journal {
  def persist(ui: UserInteraction): Future[Unit]
}
