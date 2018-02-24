package com.experiments.calvin.services

import com.outworkers.phantom.jdk8.ZonedDateTime

import scala.concurrent.Future

case class AnalyticsData(uniqueUsers: Long, clicks: Long, impressions: Long)
trait UserAnalytics {
  def dataForTimestamp(zonedDateTime: ZonedDateTime): Future[AnalyticsData]
}
