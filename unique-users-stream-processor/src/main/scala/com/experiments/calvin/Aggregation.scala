package com.experiments.calvin

import java.time.ZonedDateTime

import com.experiments.calvin.models._
import net.agkn.hll.HLL

import scala.util.hashing.MurmurHash3

trait Aggregation {
  def extractYMDH(zdt: ZonedDateTime): YMDH = {
    val year: Year   = zdt.getYear
    val month: Month = zdt.getMonth.getValue
    val day: Day     = zdt.getDayOfMonth
    val hour: Hour   = zdt.getHour
    YMDH(year, month, day, hour)
  }

  def uniqueUserIdByYMDH(es: Seq[AnalyticsEvent]): List[(YMDH, Seq[UserId])] = {
    es.groupBy(event => extractYMDH(event.timestamp))
      .mapValues(seq => seq.map(_.userId).distinct)
      .toList
  }

  def userEstimatesByYMDH(seq: Seq[(YMDH, Seq[UserId])]): Seq[(YMDH, HLL)] =
    seq.map {
      case (ymdh, userIds) =>
        val log2m    = 13
        val regWidth = 5
        val hll      = new HLL(log2m, regWidth)
        userIds
          .map(MurmurHash3.stringHash(_).toLong)
          .foreach(hll.addRaw)
        ymdh -> hll
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
