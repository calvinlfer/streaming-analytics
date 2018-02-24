package com.experiments.calvin.repositories

import scala.concurrent.Future

case class RepoEventCount(year: Int, month: Int, day: Int, hour: Int, count: Long, eventType: String)

trait EventCountByYMDH {
  def find(year: Int, month: Int, day: Int, hour: Int, eventType: String): Future[Option[RepoEventCount]]
}
