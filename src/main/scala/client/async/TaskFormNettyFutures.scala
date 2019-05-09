package scalackh.client.async

import io.netty.channel.{Channel, ChannelFuture}
import io.netty.util.concurrent.{Future, GenericFutureListener}

import monix.eval.Task
import monix.execution.CancelablePromise

object TaskFormNettyFutures {
  def fromFuture[A](f: Future[A]): Task[A] = {
    val promise = CancelablePromise[A]()

    f.addListener(new GenericFutureListener[Future[A]] {
      override def operationComplete(redeemed: Future[A]): Unit = {
        if(redeemed.isSuccess()) promise.success(redeemed.getNow())
        else promise.failure(redeemed.cause())
      }
    })

    Task.fromCancelablePromise(promise)
  }

  def fromChannelFuture(cf: ChannelFuture): Task[Channel] = fromFuture(cf).map(_ => cf.channel)
}