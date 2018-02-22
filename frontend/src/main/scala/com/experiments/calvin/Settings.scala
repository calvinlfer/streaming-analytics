package com.experiments.calvin

import akka.actor.ActorSystem
import com.typesafe.config.Config

class Settings private (config: Config) {
  object kafka {
    val uris: String       = config.getString("app.kafka.uris")
    val topic: String      = config.getString("app.kafka.topic")
    val partitionSize: Int = config.getInt("app.kafka.partition-size")
    val bufferSize: Int    = config.getInt("app.kafka.buffer-size")
  }
}

object Settings {
  def apply(config: Config): Settings      = new Settings(config)
  def apply(system: ActorSystem): Settings = new Settings(system.settings.config)
}
