package scalackh.client.core

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scalackh.protocol._
import scalackh.protocol.steps._

object ProtocolAlg {
  def contStep(os: OutputStream, in: ByteBuffer, out: ByteBuffer, currentStep: Cont): ProtocolStep = {
    val nextStep = currentStep.next(in, out)
    if(out.position > 0) {
      os.write(out.array, 0, out.position)
      os.flush()
      out.clear()
    }
    nextStep
  }

  def needsInputStep(is: InputStream, in: ByteBuffer, out: ByteBuffer, currentStep: NeedsInput): ProtocolStep = {
    if(in.position == 0 && in.limit == in.capacity) {
      val n = is.read(in.array, 0, in.capacity)
      in.position(n)
    }
    else {
      val position = in.position
      val length = in.limit - position
      System.arraycopy(in.array, position, in.array, 0, length)
      in.limit(in.capacity)
      val n = is.read(in.array, length, in.capacity - length)
      in.position(n + length)
    }
    
    in.flip()

    currentStep.next(in, out)
  }

  def iterator(is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep): Iterator[ProtocolStep] = new Iterator[ProtocolStep] {
    var currentStep: ProtocolStep = step
    var done: Boolean = false

    def hasNext: Boolean = !done

    def next(): ProtocolStep = {
      if(done) throw new IllegalStateException("Stream exhausted")
      else {
        currentStep match {
          case c: Cont =>
            currentStep = contStep(os, in, out, c)

          case ni: NeedsInput =>
            currentStep = needsInputStep(is, in, out, ni)

          case e: Emit =>
            currentStep = Cont(e.next)
            e

          case Done =>
            done = true
            currentStep = Done

          case Error(msg) =>
            done = true
            currentStep = Done
            throw new ClickhouseClientException(msg)
        }

        currentStep
      }
    }
  }
}