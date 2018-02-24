package com.experiments.calvin.repositories

import com.outworkers.phantom.Table
import com.outworkers.phantom.dsl._
import ConsistencyLevel._
import com.outworkers.phantom.keys.PartitionKey

import scala.concurrent.Future

abstract class CassandraEventCountByYMDH
    extends Table[CassandraEventCountByYMDH, RepoEventCount]
    with EventCountByYMDH {
  object year      extends IntColumn with PartitionKey
  object month     extends IntColumn with PartitionKey
  object day       extends IntColumn with PartitionKey
  object hour      extends IntColumn with PartitionKey
  object eventType extends StringColumn with PartitionKey
  object count     extends CounterColumn

  override def find(year: Int, month: Int, day: Int, hour: Int, eventType: String): Future[Option[RepoEventCount]] =
    select
      .where(_.year eqs year)
      .and(_.month eqs month)
      .and(_.day eqs day)
      .and(_.hour eqs hour)
      .and(_.eventType eqs eventType)
      .consistencyLevel_=(LOCAL_QUORUM)
      .one()
}
