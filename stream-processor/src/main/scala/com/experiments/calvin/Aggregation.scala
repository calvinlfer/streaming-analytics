package com.experiments.calvin

import java.time.ZonedDateTime

import com.experiments.calvin.models.{AnalyticsEvent, UserId}
import net.agkn.hll.HLL

trait Aggregation {
  type Year  = Int
  type Month = Int
  type Day   = Int
  type Hour  = Int
  case class YMDH(year: Year, month: Month, day: Day, hour: Hour)
  type Count = Int

  def extractYMDH(zdt: ZonedDateTime): YMDH = {
    val year: Year   = zdt.getYear
    val month: Month = zdt.getMonth.getValue
    val day: Day     = zdt.getDayOfMonth
    val hour: Hour   = zdt.getHour
    YMDH(year, month, day, hour)
  }

  def countByYMDH(es: Seq[AnalyticsEvent]): List[(YMDH, Count)] = {
    def countByYMDH(acc: Map[YMDH, Count], next: AnalyticsEvent): Map[YMDH, Count] = {
      val ymdh: YMDH   = extractYMDH(next.timestamp)
      val updatedCount = acc.getOrElse(ymdh, 0) + 1
      acc + (ymdh -> updatedCount)
    }
    es.foldLeft(Map.empty[YMDH, Count])(countByYMDH).toList
  }

  def uniqueUserIdByYMDH(es: Seq[AnalyticsEvent]): List[(YMDH, Seq[UserId])] = {
    es.groupBy(event => extractYMDH(event.timestamp))
      .mapValues(seq => seq.map(_.userId).distinct)
      .toList
  }

  def mergeHLLMappings(ma: Map[YMDH, HLL], mb: Map[YMDH, HLL]): Map[YMDH, HLL] =
    ma.foldLeft(mb) {
      case (acc, (ymdh, hll)) =>
        acc.get(ymdh) match {
          case Some(existing) =>
            existing.union(hll) // mutable :-(
            acc + (ymdh -> existing)

          case None =>
            acc + (ymdh -> hll)
        }
    }
}
