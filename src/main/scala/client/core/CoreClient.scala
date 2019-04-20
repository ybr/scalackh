package scalackh.client.core

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.{InetAddress, Socket}
import java.nio.{ByteBuffer, ByteOrder}

import scala.util.Try

import scalackh.client.{Client, Connection}
import scalackh.protocol._
import scalackh.protocol.steps._

case class CoreClient(
  address: InetAddress,
  port: Int,
  user: Option[String],
  password: Option[String],
  name: String,
  version: Version,
  settings: Map[String, Any]) extends Client {

  def connect(): Try[Connection] = Try {
    val socket = new Socket(address, port)

    val is = new BufferedInputStream(socket.getInputStream(), Client.BUFFER_SIZE)
    val os = new BufferedOutputStream(socket.getOutputStream(), Client.BUFFER_SIZE)

    val in = ByteBuffer.allocate(Client.BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN)
    val out = ByteBuffer.allocate(Client.BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN)

    val serverInfo: ServerInfo = ProtocolAlg.iterator(is, os, in, out, ProtocolSteps.sendHello(ClientInfo(
      name,
      version,
      user.getOrElse("default"),
      password.getOrElse("default"),
      ""
    )), true)
    .collect { case Emit(si: ServerInfo, _) => si }
    .next

    CoreConnection(socket, is, os, in , out, serverInfo, settings)
  }
}