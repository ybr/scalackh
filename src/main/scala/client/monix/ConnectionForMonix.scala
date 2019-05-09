package scalackh.client.monix

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scalackh.protocol._

trait ConnectionForMonix {
  def serverName(): String
  def serverVersion(): Version

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty, settings: Map[String, Any] = Map.empty)(implicit scheduler: Scheduler): Observable[Block]

  def insert(sql: String, values: Iterator[Block], settings: Map[String, Any] = Map.empty)(implicit scheduler: Scheduler): Task[Unit]

  def settings(): Map[String, Any]

  def disconnect()(implicit scheduler: Scheduler): Task[Unit]
}