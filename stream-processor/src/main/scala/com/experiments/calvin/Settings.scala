package com.experiments.calvin

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration._

class Settings private (config: Config) {
  private def getDuration(path: String): FiniteDuration = {
    val duration = config.getDuration(path)
    FiniteDuration(duration.toMillis, MILLISECONDS)
  }

  object kafka {
    val uris: String            = config.getString("app.kafka.uris")
    val topic: String           = config.getString("app.kafka.topic")
    val consumerGroupId: String = config.getString("app.kafka.consumer-group-id")
  }
}

object Settings {
  def apply(config: Config): Settings      = new Settings(config)
  def apply(system: ActorSystem): Settings = new Settings(system.settings.config)
}
