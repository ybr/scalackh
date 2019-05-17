package scalackh.client.core

import scalackh.protocol._

trait Connection {
  def serverName(): String
  def serverVersion(): Version

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty, settings: Map[String, Any] = Map.empty): Iterator[Block]

  def insert(sql: String, values: Iterator[Block], settings: Map[String, Any] = Map.empty): Unit

  def settings(): Map[String, Any]

  def disconnect(): Unit
}