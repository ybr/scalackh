package scalackh.client.derivation

import scalackh.client.core.Connection
import scalackh.protocol.Block
import scalackh.transpose.ColumnTransposer

import scala.util.Try

trait DerivationConnection {
  def underlying(): Connection
  def query[A](sql: String, externalTables: Iterator[Block] = Iterator.empty, setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A]): Iterator[Try[List[A]]]
  def insert[A](sql: String, values: Iterator[A], setting: Map[String, Any] = Map.empty)(implicit colA: ColumnTransposer[A]): Unit
  def disconnect(): Unit
}

