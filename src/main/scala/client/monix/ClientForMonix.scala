package scalackh.client.monix

import monix.eval.Task
import monix.execution.Scheduler

import scalackh.protocol._

trait ClientForMonix {
  def host(): String
  def port(): Int
  def user(): Option[String]
  def password(): Option[String]
  def name(): String
  def version(): Version
  def settings(): Map[String, Any]

  def connect()(implicit scheduler: Scheduler): Task[ConnectionForMonix]
  def shutdown()(implicit scheduler: Scheduler): Task[Unit]
}

// requires single thread executor otherwise we can have race
// between netty read handler writing in the ByteBuffer
// and the protocol decoder reading from the same ByteBuffer
// val executorS = Executors.newSingleThreadExecutor
// implicit val scheduler: Scheduler = Scheduler(executorS)
object ClientForMonix {
  val version = Version(19, 1, 6)
  val name = "scalackh"

  val DefaultBufferSize: Int = 1024 * 1024 //  1 MB

  def apply(host: String,
            port: Int,
            user: Option[String],
            password: Option[String],
            name: String,
            version: Version,
            settings: Map[String, Any]): ClientForMonix = {
    ClientForMonixImpl(host, port, user, password, name, version, settings)
  }

  def apply(host: String,
            port: Int,
            user: Option[String] = None,
            password: Option[String] = None,
            settings: Map[String, Any] = Map.empty): ClientForMonix = {
    ClientForMonix(host, port, user, password, name, version, settings)
  }
}