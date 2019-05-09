package scalackh.client.monix

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel

import java.nio.ByteBuffer

import monix.eval.Task
import monix.execution.Scheduler

import monix.reactive.Observable

import scalackh.protocol._
import scalackh.protocol.steps.ProtocolSteps

case class ConnectionForMonixImpl(
  ch: Channel,
  bbOut: ByteBuf,
  in: ByteBuffer,
  out: ByteBuffer,
  serverInfo: ServerInfo,
  settings: Map[String, Any]
) extends ConnectionForMonix {

  val serverName: String = serverInfo.name
  val serverVersion: Version = serverInfo.version

  def queryPackets(sql: String, externalTables: Iterator[Block] = Iterator.empty, querySetting: Map[String, Any])(implicit scheduler: Scheduler): Observable[ServerPacket] = {
    ProtocolAlgForNettyMonix
      .obs(ch, bbOut, in, out, ProtocolSteps.execute(sql, externalTables, Iterator.empty, settings ++ querySetting), false)
  }

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty, querySetting: Map[String, Any])(implicit scheduler: Scheduler): Observable[Block] = {
    queryPackets(sql, externalTables, querySetting).collect {
      case ServerDataBlock(block) if(block.nbColumns > 0 && block.nbRows > 0) => block
    }
  }

  def insert(sql: String, values: Iterator[Block], querySetting: Map[String, Any])(implicit scheduler: Scheduler): Task[Unit] = {
    ProtocolAlgForNettyMonix
      .obs(ch, bbOut, in, out, ProtocolSteps.execute(sql, Iterator.empty, values, settings ++ querySetting), false)
      .completedL
  }

  def disconnect()(implicit scheduler: Scheduler): Task[Unit] = TaskFormNettyFutures.fromChannelFuture(ch.close()).map(_ => ())
}