package scalackh.client.async

import com.typesafe.netty._

import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.buffer.ByteBuf
// import io.netty.channel.ChannelHandlerContext
// import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel

import java.nio.{ByteBuffer, ByteOrder}

// import monix.eval.Task
import monix.reactive.Observable
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import scalackh.protocol._
import scalackh.protocol.steps._
// import scalackh.protocol.codec._

// import org.reactivestreams._

object ClickhouseClientOverNetty {
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = 32769

    val workerGroup: EventLoopGroup = new NioEventLoopGroup()

    var observable: Option[Observable[ByteBuf]] = None

    val in = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)
    val out = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)

    try {
      val b = new Bootstrap()
      b.group(workerGroup)
      b.channel(classOf[NioSocketChannel])
      b.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      b.handler(new ChannelInitializer[SocketChannel] {
        def initChannel(ch: SocketChannel): Unit = {
          println("INIT CHANNEL")
          val publisher: HandlerPublisher[ByteBuf] = new HandlerPublisher(ch.eventLoop, classOf[ByteBuf])
          // val subscriber: HandlerSubscriber[ByteBuf] = new HandlerSubscriber(ch.eventLoop)

          observable = Some(Observable.fromReactivePublisher(publisher, 1))

          // Here you can subscriber to the publisher, and pass the subscriber to a publisher.
          ch.pipeline().addLast("toto", publisher)
          ()
        }
      })
      
      // Start the client.
      val task = for {
        channel <- TaskFormNettyFutures.fromChannelFuture(b.connect(host, port))
        _ = {
          println("Channel opened " + channel)
        }
        stepsObs = {
          val step = ProtocolSteps.sendHello(ClientInfo(
            "scalackh",
            Version(19, 1, 6),
            "default",
            "default",
            ""
          ))
          ProtocolAlgObservable.obs(observable.get, channel, in, out, step, true)
        }
        _ <- stepsObs.headL.map { l =>
          println("L " + l)
          l
        }
        // _ = println("State in " + in)
        // _ = println("State in\n" + scalackh.client.utils.HexDump.format(in.array, in.position, in.limit))
        _ = println("QUERYING...")
        queryObs = {
          // ProtocolAlgObservable.bbOut.release()
          val publisher: HandlerPublisher[ByteBuf] = new HandlerPublisher(channel.eventLoop, classOf[ByteBuf])
          // val subscriber: HandlerSubscriber[ByteBuf] = new HandlerSubscriber(ch.eventLoop)

          observable = Some(Observable.fromReactivePublisher(publisher, 1))

          // Here you can subscriber to the publisher, and pass the subscriber to a publisher.
          channel.pipeline().replace("toto", "toto", publisher)

          ProtocolAlgObservable.obs(
            observable.get,
            channel,
            in,
            out,
            ProtocolSteps.execute("SELECT * FROM toto", Iterator.empty, Iterator.empty, Map.empty),
            false
          )
        }
        _ <- queryObs.toListL.map { l =>
          println("L2 " + l)
        }
        // _ <- {
        //   println("Closing channel...")
        //   TaskFormNettyFutures.fromChannelFuture(channel.close())
        // }
      } yield {
        println("Connection closed")
      }

      Await.result(task.runToFuture.map { _ =>
        println("DONE")
      }.recoverWith {
        case t => println("EXCEPTION RAISED")
        Future.failed(t)
      }, 10 seconds)

      ()
    } finally {
      workerGroup.shutdownGracefully()
      ()
    }
  }
}