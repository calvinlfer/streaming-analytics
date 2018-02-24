package com.experiments.calvin.repositories

import java.nio.ByteBuffer

import scala.concurrent.Future

case class RepoUniqueUsers(year: Int, month: Int, day: Int, hour: Int, hllBytes: ByteBuffer)

trait UniqueUsersByYMDH {
  def find(year: Int, month: Int, day: Int, hour: Int): Future[Option[RepoUniqueUsers]]
}
