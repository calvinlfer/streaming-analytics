package com.experiments.calvin

import java.time.ZonedDateTime

import com.experiments.calvin.models.{AnalyticsEvent, UserId}
import org.scalatest.{FunSpec, MustMatchers}

class AggregationSpec extends FunSpec with MustMatchers with Aggregation {
  describe("extractYMDH") {
    it("must extract the Year, Month, Day of Month and Hour of Day (YMDH) from the time provided") {
      val zdt                          = ZonedDateTime.parse("2018-02-21T09:31:00Z")
      val YMDH(year, month, day, hour) = extractYMDH(zdt)
      year mustBe 2018
      month mustBe 2
      day mustBe 21
      hour mustBe 9
    }
  }

  describe("countByYMDH") {
    it("must categorize events by YMDH and then count them by their YMDH") {
      val zdtA = ZonedDateTime.now()
      val zdtB = zdtA.minusDays(1)
      val es = AnalyticsEvent(zdtA, "click", "bob") :: AnalyticsEvent(zdtA, "click", "jim") ::
      AnalyticsEvent(zdtB, "click", "john") :: Nil

      val result: Seq[(YMDH, Count)] = countByYMDH(es)
      result.length mustBe 2
      result must contain(extractYMDH(zdtA) -> 2)
      result must contain(extractYMDH(zdtB) -> 1)
    }
  }

  describe("uniqueUserIdByYMDH") {
    it("must come up with a list of unique usernames by YMDH") {
      val zdtA = ZonedDateTime.now()
      val zdtB = zdtA.minusDays(1)
      val es = AnalyticsEvent(zdtA, "click", "bob") :: AnalyticsEvent(zdtA, "click", "bob") ::
      AnalyticsEvent(zdtB, "click", "john") :: Nil

      val result: Seq[(YMDH, Seq[UserId])] = uniqueUserIdByYMDH(es)
      result.length mustBe 2
      result must contain(extractYMDH(zdtA) -> List("bob"))
      result must contain(extractYMDH(zdtB) -> List("john"))
    }
  }
}
