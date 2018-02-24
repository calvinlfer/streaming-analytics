package com.experiments.calvin.services

import com.experiments.calvin.repositories.UniqueUsersByYMDH
import com.outworkers.phantom.jdk8.ZonedDateTime
import net.agkn.hll.HLL

import scala.concurrent.{ExecutionContext, Future}

class UserAnalyticsImpl(uniqueUsersRepo: UniqueUsersByYMDH)(implicit ec: ExecutionContext) extends UserAnalytics {
  override def dataForTimestamp(zonedDateTime: ZonedDateTime): Future[AnalyticsData] = {
    val year = zonedDateTime.getYear
    val month = zonedDateTime.getMonth.getValue
    val day = zonedDateTime.getDayOfMonth
    val hour = zonedDateTime.getHour

    val uniqueUserResults =
      uniqueUsersRepo.find(year, month, day, hour)
        .map(opt => opt.fold(ifEmpty = 0L) { result =>
          val hll = HLL.fromBytes(result.hllBytes.array())
          hll.cardinality()
        })
    uniqueUserResults.map(estimate => AnalyticsData(uniqueUsers = estimate, clicks = 0, impressions = 0))
  }
}
