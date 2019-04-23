package scalackh.client

import java.net.InetAddress

import scala.util.Try

import scalackh.client.core._
import scalackh.protocol._

trait Client {
  def address(): InetAddress
  def port(): Int
  def user(): Option[String]
  def password(): Option[String]
  def name(): String
  def version(): Version
  def settings(): Map[String, Any]

  def connect(): Try[Connection]
}

object Client {
  val version = Version(19, 1, 6)
  val name = "scalackh"

  val DefaultBufferSize: Int = 1024 * 1024 //  1 MB

  def apply(address: InetAddress,
            port: Int,
            user: Option[String],
            password: Option[String],
            name: String,
            version: Version,
            settings: Map[String, Any]): Client = {
    CoreClient(address, port, user, password, name, version, settings)
  }

  def apply(host: String, port: Int, user: Option[String] = None, password: Option[String] = None, settings: Map[String, Any] = Map.empty): Try[Client] = Try {
    val address: InetAddress = InetAddress.getByName(host)
    Client(address, port, user, password, name, version, settings)
  }
}

