package scalackh.client

import scalackh.transpose._

import scalackh.client.sync._
import scalackh.protocol._
import scalackh.protocol.steps.ProtocolSteps

trait DerivationConnection {
  def query[A](sql: String)(implicit colA: ColumnTransposer[A]): Iterator[A]
  def query[A, B](sql: String, externalTables: Iterator[B])(implicit colA: ColumnTransposer[A], colB: ColumnTransposer[B]): Iterator[A]
  def insert[A](sql: String, values: Iterator[A])(implicit colA: ColumnTransposer[A]): Unit
  def disconnect(): Unit
}

class DerivationConnectionImpl(val connection: Connection) extends DerivationConnection {
  def query[A](sql: String)(implicit colA: ColumnTransposer[A]): Iterator[A] = ???
  def query[A, B](sql: String, externalTables: Iterator[B])(implicit colA: ColumnTransposer[A], colB: ColumnTransposer[B]): Iterator[A] = ???

  def insert[A](sql: String, values: Iterator[A])(implicit colA: ColumnTransposer[A]): Unit = {
    val blocks = values.grouped(ProtocolSteps.MAX_ROWS_IN_BLOCK).map { group =>
      val valuesForBlock = group.toList
      val columns: List[ColumnData] = colA.toColumns(valuesForBlock)
      Block(None, BlockInfo.empty, columns.length, valuesForBlock.length, columns.map(Column("", _)))
    }

    connection.insert(sql, blocks)
  }

  def disconnect(): Unit = connection.disconnect()
}