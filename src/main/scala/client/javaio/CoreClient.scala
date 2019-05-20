package scalackh.client.javaio

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.{InetAddress, Socket}
import java.nio.{ByteBuffer, ByteOrder}

import scala.util.Try

import scalackh.protocol._
import scalackh.protocol.steps._

case class CoreClient(
  address: InetAddress,
  port: Int,
  user: String,
  password: String,
  database: String,
  name: String,
  version: Version,
  settings: Map[String, Any]) extends Client {

  def connect(): Try[Connection] = Try {
    val socket = new Socket(address, port)

    val is = new BufferedInputStream(socket.getInputStream(), Client.DefaultBufferSize)
    val os = new BufferedOutputStream(socket.getOutputStream(), Client.DefaultBufferSize)

    val in = ByteBuffer.allocate(Client.DefaultBufferSize).order(ByteOrder.LITTLE_ENDIAN)
    val out = ByteBuffer.allocate(Client.DefaultBufferSize).order(ByteOrder.LITTLE_ENDIAN)

    val serverInfo: ServerInfo = ProtocolAlg.iterator(is, os, in, out, ProtocolSteps.sendHello(ClientInfo(
      name,
      version,
      database,
      user,
      password,
    )), true)
    .collect { case Emit(si: ServerInfo, _) => si }
    .next

    CoreConnection(socket, is, os, in , out, serverInfo, settings)
  }
}