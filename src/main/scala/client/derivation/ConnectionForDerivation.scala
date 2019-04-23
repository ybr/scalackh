package scalackh.client.derivation

import scalackh.client.Connection
import scalackh.math.UInt64
import scalackh.protocol._
import scalackh.transpose.ColumnTransposer

import scala.util.Try

case class ConnectionForDerivation(underlying: Connection) extends DerivationConnection {
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