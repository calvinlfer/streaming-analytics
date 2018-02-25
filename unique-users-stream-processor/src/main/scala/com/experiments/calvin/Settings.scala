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

  object cassandra {
    val host: String           = config.getString("app.cassandra.host")
    val port: Int              = config.getInt("app.cassandra.port")
    val keyspace: String       = config.getString("app.cassandra.keyspace")
    val trustStorePath: String = config.getString("app.cassandra.truststore-path")
    val trustStorePass: String = config.getString("app.cassandra.truststore-password")
    val username: Option[String] = {
      val user = config.getString("app.cassandra.username")
      if (user.nonEmpty) Some(user) else None
    }
    val password: Option[String] = {
      val pass = config.getString("app.cassandra.password")
      if (pass.nonEmpty) Some(pass) else None
    }
    val autoInitKeyspace: Boolean = config.getBoolean("app.cassandra.initialize-keyspace")
  }

  object batch {
    val maxElements: Int            = config.getInt("app.batch.max-elements")
    val maxDuration: FiniteDuration = getDuration("app.batch.max-time-to-wait")
    val updateConcurrency: Int      = config.getInt("app.batch.update-concurrency")
  }
}

object Settings {
  def apply(config: Config): Settings      = new Settings(config)
  def apply(system: ActorSystem): Settings = new Settings(system.settings.config)
}
