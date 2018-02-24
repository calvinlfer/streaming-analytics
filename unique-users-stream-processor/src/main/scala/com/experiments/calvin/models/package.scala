package com.experiments.calvin

import java.time.ZonedDateTime

import akka.kafka.ConsumerMessage

package object models {
  type UserId = String
  case class AnalyticsEvent(timestamp: ZonedDateTime, event: String, userId: UserId)

  type Year  = Int
  type Month = Int
  type Day   = Int
  type Hour  = Int
  case class YMDH(year: Year, month: Month, day: Day, hour: Hour)

  type Count = Int

  case class KafkaEnvelope[A](payload: A, offset: ConsumerMessage.CommittableOffset) {
    def map[B](fn: A => B): KafkaEnvelope[B] = copy(fn(payload), offset)
  }
}
