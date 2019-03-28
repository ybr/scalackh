package scalackh.client.derivation

import scalackh.protocol._
import scalackh.transpose.ColumnTransposer

import scala.util.Try

trait DerivationConnection {
  def query[A](sql: String, externalTables: Iterator[Block] = Iterator.empty)(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]]
  def insert[A](sql: String, values: Iterator[A])(implicit colA: ColumnTransposer[A]): Unit
  def disconnect(): Unit
}

