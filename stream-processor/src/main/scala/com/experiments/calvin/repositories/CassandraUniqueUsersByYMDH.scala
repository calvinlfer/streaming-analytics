package com.experiments.calvin.repositories

import java.nio.ByteBuffer

import com.outworkers.phantom.Table
import com.outworkers.phantom.keys.PartitionKey
import com.outworkers.phantom.dsl._
import ConsistencyLevel._

import scala.concurrent.Future

abstract class CassandraUniqueUsersByYMDH
    extends Table[CassandraUniqueUsersByYMDH, RepoUniqueUsers]
    with UniqueUsersByYMDH {
  object year     extends IntColumn with PartitionKey
  object month    extends IntColumn with PartitionKey
  object day      extends IntColumn with PartitionKey
  object hour     extends IntColumn with PartitionKey
  object hllBytes extends BlobColumn

  override def find(year: Int, month: Int, day: Int, hour: Int): Future[Option[RepoUniqueUsers]] =
    select
      .where(_.year eqs year)
      .and(_.month eqs month)
      .and(_.day eqs day)
      .and(_.hour eqs hour)
      .consistencyLevel_=(LOCAL_QUORUM)
      .one()

  override def persist(year: Int, month: Int, day: Int, hour: Int, hllBytes: ByteBuffer): Future[RepoUniqueUsers] = {
    insert
      .value(_.year, year)
      .value(_.month, month)
      .value(_.day, day)
      .value(_.hour, hour)
      .value(_.hllBytes, hllBytes)
      .consistencyLevel_=(LOCAL_QUORUM)
      .future()
      .map(_ => RepoUniqueUsers(year, month, day, hour, hllBytes))
  }
}
