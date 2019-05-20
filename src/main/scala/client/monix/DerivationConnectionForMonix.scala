package scalackh.client.monix

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scalackh.math.UInt64
import scalackh.transpose.{ColumnTransposer, DerivationUtils}
import scalackh.protocol.Block

trait DerivationConnectionForMonix {
  def underlying(): ConnectionForMonix
  def settings(): Map[String, Any]
  def query[A](sql: String, externalTables: Iterator[Block] = Iterator.empty, setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A], scheduler: Scheduler): Observable[List[A]]
  def insert[A](sql: String, values: Iterator[A], setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A], scheduler: Scheduler): Task[Unit]
  def disconnect()(implicit scheduler: Scheduler): Task[Unit]
}

class DerivationConnectionForMonixImpl(val underlying: ConnectionForMonix) extends DerivationConnectionForMonix {
  def settings(): Map[String, Any] = underlying.settings

  def query[A](sql: String, externalTables: Iterator[Block], settings: Map[String, Any])(implicit colA: ColumnTransposer[A], scheduler: Scheduler): Observable[List[A]] = {
    underlying.query(sql, externalTables, settings).flatMap { block =>
      Observable.fromTry(colA.fromColumnsData(block.columns.map(_.data)))
    }
  }

  def insert[A](sql: String, values: Iterator[A], settings: Map[String, Any])(implicit colA: ColumnTransposer[A], scheduler: Scheduler): Task[Unit] = {
    val maxInsertBlockSize: Int = settings.get("max_insert_block_size").collect { case ui64: UInt64 => ui64.unsafeLong.toInt }.getOrElse(65535)
    val blocks = DerivationUtils.blockIterator(values, maxInsertBlockSize)
    underlying.insert(sql, blocks, settings)
  }

  def disconnect()(implicit scheduler: Scheduler): Task[Unit] = underlying.disconnect()
}