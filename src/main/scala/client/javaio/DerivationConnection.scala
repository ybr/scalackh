package scalackh.client.javaio

import scalackh.math.UInt64
import scalackh.protocol.Block
import scalackh.transpose.{ColumnTransposer, DerivationUtils}

import scala.util.Try

trait DerivationConnection {
  def underlying(): Connection
  def query[A](sql: String, externalTables: Iterator[Block] = Iterator.empty, setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]]
  def insert[A](sql: String, values: Iterator[A], setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A]): Unit
  def disconnect(): Unit
}

case class DerivationConnectionImpl(underlying: Connection) extends DerivationConnection {
  def query[A](sql: String, externalTables: Iterator[Block], settings: Map[String, Any])(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]] = {
    underlying.query(sql, externalTables, settings).map { block =>
      colA.fromColumnsData(block.columns.map(_.data))
    }
  }

  def insert[A](sql: String, values: Iterator[A], settings: Map[String, Any])(implicit colA: ColumnTransposer[A]): Unit = {
    val maxInsertBlockSize: Int = settings.get("max_insert_block_size").collect { case ui64: UInt64 => ui64.unsafeLong.toInt }.getOrElse(65535)
    val blocks = DerivationUtils.blockIterator(values, maxInsertBlockSize)
    underlying.insert(sql, blocks, settings)
  }

  def disconnect(): Unit = underlying.disconnect()
}