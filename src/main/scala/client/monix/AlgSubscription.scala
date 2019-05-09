package scalackh.client.monix

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel

import java.nio.ByteBuffer

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import org.reactivestreams._
import scalackh.protocol._
import scalackh.protocol.steps._

class AlgSubscription(subscriber: Subscriber[_ >: ServerPacket], bbs: Observable[ByteBuf], ch: Channel, bbOut: ByteBuf, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean)(implicit scheduler: Scheduler) extends Subscription {
  var currentStep: ProtocolStep = step

  def cancel(): Unit = {
    // Cancel not managed
  }

  def request(n: Long): Unit = applyStep()

  bbs.foreach { bb =>
    Task {
      currentStep = ProtocolAlgForNettyMonix.needsInputStep(bb, in, out, currentStep, first)
      applyStep()
    }
    .executeOn(scheduler)
    .runToFuture(scheduler)
    ()
  }

  def applyStep(): Unit = {
    currentStep match {
      case c: Cont =>
        ProtocolAlgForNettyMonix.contStep(ch, bbOut, in, out, c).foreach { nextStep =>
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