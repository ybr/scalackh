package scalackh.client.async

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.Channel

import java.nio.ByteBuffer

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import org.reactivestreams._
import scalackh.protocol._
import scalackh.protocol.steps._

object ProtocolAlgObservable {
  val bbOut: ByteBuf = Unpooled.buffer(1024 * 1024, 1024 * 1024)
  bbOut.retain()

  def contStep(ch: Channel, in: ByteBuffer, out: ByteBuffer, currentStep: Cont): Task[ProtocolStep] = {
    val nextStep = currentStep.next(in, out)
    if(out.position > 0) {
      out.flip()
      println("<<<<<<<<<<<<<<<< SENDING\n" + scalackh.client.utils.HexDump.format(out.array, out.position, out.limit))
      bbOut.writeBytes(out)
  
      println("Sending data ")

      TaskFormNettyFutures.fromFuture(ch.writeAndFlush(bbOut)).map { _ =>
        out.clear()
        bbOut.clear()
        println("Data sent")
        nextStep    
      }
    }
    else Task.pure(nextStep)
  }

  def readInput(bb: ByteBuf, in: ByteBuffer, first: Boolean): Unit = {
    println("Signet 3 " + first + " " + in)
    if(first) {
      val length = Math.min(in.capacity, bb.writerIndex - bb.readerIndex)
      bb.readBytes(in.array, 0, length)
      println("Signet 4 " + in + " " + length)
      in.position(length)
      println("Signet 5")
      in.flip()
      println("Signet 6")
    }
    else {
      val position = in.position
      val length = in.limit - position
      println("length " + length)
      System.arraycopy(in.array, position, in.array, 0, length)
      val n = Math.min(in.capacity - length, bb.writerIndex - bb.readerIndex)
      println("n " + n)
      println("in.n " + (in.capacity - length))
      println("bb.n " + (bb.writerIndex - bb.readerIndex))
      in.limit(in.capacity)
      bb.readBytes(in.array, length, n)
      println("Signet 7")
      in.position(n + length)
      println("Signet 8")
      in.flip()
    }
    bb.discardReadBytes()
    println(s">>>>>>>>>>>>>> RECEIVED $in\n" + scalackh.client.utils.HexDump.format(in.array, in.position, in.limit))
    ()
  }

  def needsInputStep(bb: ByteBuf, in: ByteBuffer, out: ByteBuffer, currentStep: ProtocolStep, first: Boolean): ProtocolStep = {
    println("Signet 1")
    readInput(bb, in, first)
    println("Signet 2")
    currentStep match {
      case NeedsInput(nextStep) => nextStep(in, out)
      case other => other
    }
  }

  def obs(bbs: Observable[ByteBuf], ch: Channel, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit schedule: Scheduler): Observable[ServerPacket] = {
    Observable.fromReactivePublisher(new ProtocolStepPublisher(bbs, ch, in, out, step, first), 1)
  }
}

class LogSubscriber[A](underlying: Subscriber[A]) extends Subscriber[A] {
  def onSubscribe(s: Subscription): Unit = {
    println("ON SUBSCRIBE")
    underlying.onSubscribe(s)
  }

  def onNext(a: A): Unit = {
    println("ON NEXT " + a)
    underlying.onNext(a)
  }

  def onComplete(): Unit = {
    println("ON COMPLETE")
    underlying.onComplete()
  }

  def onError(t: Throwable): Unit = {
    println("ON ERROR " + t)
    underlying.onError(t)
  }
}

class ProtocolStepPublisher(bbs: Observable[ByteBuf], ch: Channel, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit schedule: Scheduler) extends Publisher[ServerPacket] {
  def subscribe(s: Subscriber[_ >: ServerPacket]): Unit = {
    // val s1 = new LogSubscriber(s)
    val s1 = s
    s1.onSubscribe(new AlgSubscription(s1, bbs, ch, in, out, step, first))
  }
}

class AlgSubscription(subscriber: Subscriber[_ >: ServerPacket], bbs: Observable[ByteBuf], ch: Channel, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit schedule: Scheduler) extends Subscription {
  var currentStep: ProtocolStep = step

  def cancel(): Unit = {
    println("Cancel not managed")
  }

  def request(n: Long): Unit = applyStep()

  bbs.foreach { bb =>
    println("Received data before " + in)
    println(bb)

    currentStep = ProtocolAlgObservable.needsInputStep(bb, in, out, currentStep, first)
    println("Received data after " + in)    
    applyStep()
  }

  def applyStep(): Unit = {
    currentStep match {
      case c: Cont =>
        ProtocolAlgObservable.contStep(ch, in, out, c).foreach { nextStep =>
          currentStep = nextStep
          applyStep()
        }

      case _: NeedsInput =>
        ch.read()
        ()

      case Emit(x: ServerException, _) =>
        currentStep = Done
        subscriber.onError(new ClickhouseServerException(x))

      case Emit(packet, nextStep) =>
        currentStep = Cont(nextStep)
        subscriber.onNext(packet)

      case Done => subscriber.onComplete()

      case Error(msg) => subscriber.onError(new ClickhouseClientException(msg))
    }
  }
}