package scalackh.client.monix

import com.typesafe.netty.HandlerPublisher

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

import java.nio.{ByteBuffer, ByteOrder}

import monix.eval.Task
import monix.execution.Scheduler

import scalackh.protocol._
import scalackh.protocol.steps._

case class ClientForMonixImpl(
  host: String,
  port: Int,
  user: String,
  password: String,
  database: String,
  name: String,
  version: Version,
  settings: Map[String, Any]) extends ClientForMonix {

  val eventLoopGroup: EventLoopGroup = new NioEventLoopGroup()

  def connect()(implicit scheduler: Scheduler): Task[ConnectionForMonix] = {
    val bbOut: ByteBuf = Unpooled.buffer(ClientForMonix.DefaultBufferSize, ClientForMonix.DefaultBufferSize)

    val b = new Bootstrap()
    b.group(eventLoopGroup)
    b.channel(classOf[NioSocketChannel])
    b.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    b.handler(new ChannelInitializer[SocketChannel] {
      def initChannel(ch: SocketChannel): Unit = {
        val publisher: HandlerPublisher[ByteBuf] = new HandlerPublisher(ch.eventLoop, classOf[ByteBuf])
        ch.pipeline().addLast(ProtocolAlgForNettyMonix.ListenerName, publisher)
        ()
      }
    })

    val in = ByteBuffer.allocate(ClientForMonix.DefaultBufferSize).order(ByteOrder.LITTLE_ENDIAN)
    val out = ByteBuffer.allocate(ClientForMonix.DefaultBufferSize).order(ByteOrder.LITTLE_ENDIAN)

    val step = ProtocolSteps.sendHello(ClientInfo(
      name,
      version,
      database,
      user,
      password
    ))

    for {
      channel <- TaskFormNettyFutures.fromChannelFuture(b.connect(host, port))
      serverInfo <- ProtocolAlgForNettyMonix
        .obs(channel, bbOut, in, out, step, true)
        .headL
        .flatMap {
          case si: ServerInfo => Task(si)
          case other => Task.raiseError(new ClickhouseClientException(s"Unexpected packet: $other"))
        }
    } yield ConnectionForMonixImpl(channel, bbOut, in, out, serverInfo, settings)
  }

  def shutdown()(implicit scheduler: Scheduler): Task[Unit] = TaskFormNettyFutures.fromFuture(eventLoopGroup.shutdownGracefully()).map(_ => ())
}