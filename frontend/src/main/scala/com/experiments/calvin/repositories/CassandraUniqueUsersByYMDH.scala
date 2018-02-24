package com.experiments.calvin.repositories
import com.outworkers.phantom.Table
import com.outworkers.phantom.dsl._
import ConsistencyLevel._
import com.outworkers.phantom.keys.PartitionKey

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
}
