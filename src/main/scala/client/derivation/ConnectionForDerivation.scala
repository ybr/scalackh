package scalackh.client.derivation

import scalackh.client.Connection
import scalackh.protocol._
import scalackh.transpose.ColumnTransposer

import scala.util.Try

case class ConnectionForDerivation(underlying: Connection) extends DerivationConnection {

  def query[A](sql: String, externalTables: Iterator[Block])(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]] = {
    underlying.query(sql, externalTables).map { block =>
      colA.fromColumnsData(block.columns.map(_.data))
    }
  }

  def insert[A](sql: String, values: Iterator[A])(implicit colA: ColumnTransposer[A]): Unit = {
    val blocks = DerivationUtils.blockIterator(values)
    underlying.insert(sql, blocks)
  }

  def disconnect(): Unit = underlying.disconnect()
}