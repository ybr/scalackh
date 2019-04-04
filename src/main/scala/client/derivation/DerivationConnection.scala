package scalackh.client.derivation

import scalackh.protocol.Block
import scalackh.client.Connection
import scalackh.transpose.ColumnTransposer

import scala.util.Try

trait DerivationConnection {
  def underlying(): Connection
  def query[A](sql: String, externalTables: Iterator[Block] = Iterator.empty)(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]]
  def insert[A](sql: String, values: Iterator[A])(implicit colA: ColumnTransposer[A]): Unit
  def disconnect(): Unit
}

