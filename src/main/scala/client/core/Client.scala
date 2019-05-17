package scalackh.client.core

import java.net.InetAddress

import scala.util.Try

import scalackh.protocol._

trait Client {
  def address(): InetAddress
  def port(): Int
  def user(): String
  def password(): String
  def database(): String
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
            user: String,
            password: String,
            database: String,
            name: String,
            version: Version,
            settings: Map[String, Any]): Client = {
    CoreClient(address, port, user, password, database, name, version, settings)
  }

  def apply(host: String, port: Int, user: String = "default", password: String = "", database: String = "default", settings: Map[String, Any] = Map.empty): Try[Client] = Try {
    val address: InetAddress = InetAddress.getByName(host)
    Client(address, port, user, password, database, name, version, settings)
  }
}

