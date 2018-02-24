package com.experiments.calvin.repositories
import scala.concurrent.Future

case class RepoEventCount(year: Int, month: Int, day: Int, hour: Int, count: Long, eventType: String)

trait EventCountByYMDH {
  def find(year: Int, month: Int, day: Int, hour: Int): Future[Option[RepoEventCount]]
  def persist(year: Int, month: Int, day: Int, hour: Int, eventType: String, countToAdd: Long): Future[RepoEventCount]
}
