package scalackh.client.core

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scalackh.protocol._
import scalackh.protocol.steps._

object ProtocolAlg {
  def iterator(is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep): Iterator[ProtocolStep] = new Iterator[ProtocolStep] {
    var currentStep: ProtocolStep = step
    var done: Boolean = false

    def hasNext: Boolean = !done

    def next(): ProtocolStep = {
      if(done) throw new IllegalStateException("Stream exhausted")
      else {
        currentStep match {
          case Cont(step) =>
            val nextStep = step(in, out)
            if(out.position > 0) {
              os.write(out.array, 0, out.position)
              os.flush()
              out.clear()
            }
            currentStep = nextStep

          case NeedsInput(step) =>
            in.clear()

            // wait for input
            val n = is.read(in.array(), 0, in.capacity)
            in.position(n)
            in.flip()

            currentStep = step(in, out)

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