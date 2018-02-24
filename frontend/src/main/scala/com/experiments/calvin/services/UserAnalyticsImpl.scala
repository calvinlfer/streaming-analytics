package com.experiments.calvin.services

import com.experiments.calvin.repositories.{EventCountByYMDH, RepoEventCount, UniqueUsersByYMDH}
import com.outworkers.phantom.jdk8.ZonedDateTime
import net.agkn.hll.HLL

import scala.concurrent.{ExecutionContext, Future}

class UserAnalyticsImpl(uniqueUsersRepo: UniqueUsersByYMDH, countsRepo: EventCountByYMDH)(implicit ec: ExecutionContext)
    extends UserAnalytics {
  private def extractResult[A](fo: Future[Option[A]])(fn: A => Long): Future[Long] =
    fo.map(opt => opt.fold(ifEmpty = 0L)(fn))

  override def dataForTimestamp(zonedDateTime: ZonedDateTime): Future[AnalyticsData] = {
    val year  = zonedDateTime.getYear
    val month = zonedDateTime.getMonth.getValue
    val day   = zonedDateTime.getDayOfMonth
    val hour  = zonedDateTime.getHour

    val uniqueUserCount = extractResult(uniqueUsersRepo.find(year, month, day, hour)) { result =>
      val hll = HLL.fromBytes(result.hllBytes.array())
      hll.cardinality()
    }
    val clicksCount = extractResult(countsRepo.find(year, month, day, hour, "click"))(_.count)
    val imprCount   = extractResult(countsRepo.find(year, month, day, hour, "impression"))(_.count)

    for {
      uniqueUsers <- uniqueUserCount
      clicks      <- clicksCount
      imprs       <- imprCount
    } yield AnalyticsData(uniqueUsers, clicks, imprs)
  }
}
