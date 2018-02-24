package com.experiments.calvin

import java.time.ZonedDateTime

import com.experiments.calvin.models._

trait Aggregation {
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
}
