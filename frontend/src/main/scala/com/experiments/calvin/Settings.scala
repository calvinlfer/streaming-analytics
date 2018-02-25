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
    val uris: String       = config.getString("app.kafka.uris")
    val topic: String      = config.getString("app.kafka.topic")
    val partitionSize: Int = config.getInt("app.kafka.partition-size")
    val bufferSize: Int    = config.getInt("app.kafka.buffer-size")

    object actor {
      val minInstances: Int = config.getInt("app.kafka.actor.min-instances")
      val maxInstances: Int = config.getInt("app.kafka.actor.max-instances")
    }

    object service {
      val timeout: FiniteDuration = getDuration("app.kafka.service.ask-timeout")
    }

    object backoff {
      val min: FiniteDuration = getDuration("app.kafka.backoff.min")
      val max: FiniteDuration = getDuration("app.kafka.backoff.max")
      val randomness: Double  = config.getInt("app.kafka.backoff.randomness")
    }
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

  object http {
    val host: String = config.getString("app.http.host")
    val port: Int    = config.getInt("app.http.port")
  }
}

object Settings {
  def apply(config: Config): Settings      = new Settings(config)
  def apply(system: ActorSystem): Settings = new Settings(system.settings.config)
}
