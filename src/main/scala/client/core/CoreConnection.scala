package scalackh.client.core

import java.io.{InputStream, OutputStream}
import java.net.Socket
import java.nio.ByteBuffer

import scalackh.protocol._
import scalackh.protocol.steps.{Emit, ProtocolSteps}

case class CoreConnection(
  socket: Socket,
  is: InputStream,
  os: OutputStream,
  in: ByteBuffer,
  out: ByteBuffer,
  serverInfo: ServerInfo,
  settings: Map[String, Any]
) extends Connection {

  val serverName: String = serverInfo.name
  val serverVersion: Version = serverInfo.version

  def queryPackets(sql: String, externalTables: Iterator[Block] = Iterator.empty, querySetting: Map[String, Any]): Iterator[ServerPacket] = {
    ProtocolAlg
      .iterator(is, os, in, out, ProtocolSteps.execute(sql, externalTables, Iterator.empty, settings ++ querySetting), false)
      .collect {
        case Emit(x: ServerException, _) => throw new ClickhouseServerException(x)
        case Emit(packet, _) => packet
      }
  }

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty, querySetting: Map[String, Any]): Iterator[Block] = {
    queryPackets(sql, externalTables, querySetting).collect {
      case ServerDataBlock(block) if(block.nbColumns > 0 && block.nbRows > 0) => block
    }
  }

  def insert(sql: String, values: Iterator[Block], querySetting: Map[String, Any]): Unit = {
    ProtocolAlg.iterator(is, os, in, out, ProtocolSteps.execute(sql, Iterator.empty, values, settings ++ querySetting), false)
    .foreach(_ => ())
  }

  def disconnect(): Unit = socket.close()
}