package scalackh.client.monix

import com.typesafe.netty.HandlerPublisher

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel

import java.nio.ByteBuffer

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import org.reactivestreams._
import scalackh.protocol._
import scalackh.protocol.steps._

object ProtocolAlgForNettyMonix {
  val ListenerName: String = "netty-monix-handler"

  def contStep(ch: Channel, bbOut: ByteBuf, in: ByteBuffer, out: ByteBuffer, currentStep: Cont): Task[ProtocolStep] = {
    val nextStep = currentStep.next(in, out)
    if(out.position > 0) {
      out.flip()
      bbOut.writeBytes(out)

      TaskFormNettyFutures.fromFuture(ch.writeAndFlush(bbOut)).map { _ =>
        out.clear()
        bbOut.clear()
        nextStep    
      }
    }
    else Task.pure(nextStep)
  }

  def readInput(bb: ByteBuf, in: ByteBuffer, first: Boolean): Unit = {
    if(first) {
      val length = Math.min(in.capacity, bb.writerIndex - bb.readerIndex)
      bb.readBytes(in.array, 0, length)
      in.position(length)
      in.flip()
    }
    else {
      val position = in.position
      val length = in.limit - position
      System.arraycopy(in.array, position, in.array, 0, length)
      val n = Math.min(in.capacity - length, bb.writerIndex - bb.readerIndex)
      in.limit(in.capacity)
      bb.readBytes(in.array, length, n)
      in.position(n + length)
      in.flip()
    }
    bb.discardReadBytes()
    ()
  }

  def needsInputStep(bb: ByteBuf, in: ByteBuffer, out: ByteBuffer, currentStep: ProtocolStep, first: Boolean): ProtocolStep = {
    readInput(bb, in, first)
    currentStep match {
      case NeedsInput(nextStep) => nextStep(in, out)
      case other => other
    }
  }

  def obs(ch: Channel, bbOut: ByteBuf, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit scheduler: Scheduler): Observable[ServerPacket] = {
    bbOut.retain(1) // must retain netty byte buffer otherwise it is released
    val publisher: HandlerPublisher[ByteBuf] = new HandlerPublisher(ch.eventLoop, classOf[ByteBuf])
    val bbs: Observable[ByteBuf] = Observable.fromReactivePublisher(publisher, 1)
    ch.pipeline().replace(ListenerName, ListenerName, publisher)
    Observable.fromReactivePublisher(new ProtocolStepPublisher(bbs, ch, bbOut, in, out, step, first), 1)
  }
}

class ProtocolStepPublisher(bbs: Observable[ByteBuf], ch: Channel, bbOut: ByteBuf, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit scheduler: Scheduler) extends Publisher[ServerPacket] {
  def subscribe(s: Subscriber[_ >: ServerPacket]): Unit = {
    s.onSubscribe(new AlgSubscription(s, bbs, ch, bbOut, in, out, step, first))
  }
}