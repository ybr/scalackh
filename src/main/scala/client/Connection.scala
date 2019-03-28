package scalackh.client

import scalackh.protocol._

trait Connection {
  def serverName(): String
  def serverVersion(): Version

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty): Iterator[Block]

  def insert(sql: String, values: Iterator[Block]): Unit

  def disconnect(): Unit
}