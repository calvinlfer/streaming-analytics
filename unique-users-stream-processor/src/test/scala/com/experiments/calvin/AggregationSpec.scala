package com.experiments.calvin

import java.time.ZonedDateTime

import com.experiments.calvin.models._
import net.agkn.hll.HLL
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

  describe("userEstimatesByYMDH") {
    it("estimating cardinality should be the same as the actual cardinality for average numbers") {
      val zdt             = ZonedDateTime.now()
      val ymdh            = extractYMDH(zdt)
      val userIds         = Range(0, 100).inclusive.map(_.toString)
      val (_, hll) :: Nil = userEstimatesByYMDH(Seq(ymdh -> userIds))
      hll.cardinality() mustBe 101
    }
  }

  describe("mergeHLLMappings") {
    it("merging cardinality mappings with itself should have no effect") {
      val zdt     = ZonedDateTime.now()
      val ymdh    = extractYMDH(zdt)
      val userIds = Range(0, 100).inclusive.map(_.toString)
      val result  = userEstimatesByYMDH(Seq(ymdh -> userIds)).toMap

      val mergedResult: Map[YMDH, HLL] = mergeHLLMappings(result, result)
      mergedResult.size mustBe 1
      mergedResult(ymdh).cardinality() mustBe 101
    }
  }
}
