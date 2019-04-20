package scalackh.client.core

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scalackh.protocol._
import scalackh.protocol.steps._

object ProtocolAlg {
  def contStep(is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, currentStep: Cont): ProtocolStep = {
    val nextStep = currentStep.next(in, out)
    if(out.position > 0) {
      os.write(out.array, 0, out.position)
      os.flush()
      out.clear()
    }
    if(is.available() > 0) readInput(is, in)
    nextStep
  }

  def readInput(is: InputStream, in: ByteBuffer): Unit = {
    val position = in.position
    val length = in.limit - position
    System.arraycopy(in.array, position, in.array, 0, length)
    in.limit(in.capacity)
    val n = is.read(in.array, length, in.capacity - length)
    in.position(n + length)
    in.flip()
    ()
  }

  def needsInputStep(is: InputStream, in: ByteBuffer, out: ByteBuffer, currentStep: NeedsInput, first: Boolean): ProtocolStep = {
    if(first) {
      val n = is.read(in.array, 0, in.capacity)
      in.position(n)
      in.flip()
    }
    else readInput(is, in)

    currentStep.next(in, out)
  }

  def iterator(is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep, first: Boolean): Iterator[ProtocolStep] = new Iterator[ProtocolStep] {
    var currentStep: ProtocolStep = step
    var done: Boolean = false

    def hasNext: Boolean = !done

    def next(): ProtocolStep = {
      if(done) throw new IllegalStateException("Stream exhausted")
      else {
        currentStep match {
          case c: Cont =>
            currentStep = contStep(is, os, in, out, c)

          case ni: NeedsInput =>
            currentStep = needsInputStep(is, in, out, ni, first)

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