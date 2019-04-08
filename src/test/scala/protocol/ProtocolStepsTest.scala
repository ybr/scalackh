package scalackh.protocol.steps

import scalackh.protocol._

import java.nio.ByteBuffer

import minitest._

object ProtocolStepsTest extends SimpleTestSuite {
  test("receiveResult with multipacket") {
    val data = ByteBuffer.wrap(Array(
                      0x01, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x05, 0x04, 0x64, 0x61, 0x74,
                      0x65, 0x04, 0x44, 0x61, 0x74, 0x65, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46,
                      0x06, 0x05, 0x01, 0x0a, 0x00, 0x00, 0x01, 0x03, 0x05, 0x0a, 0x80, 0x40, 0x01, 0x00, 0x01, 0x00,
                      0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x05).map(_.toByte))

    val emptyBuf = ByteBuffer.allocate(0)

    val initStep = ProtocolSteps.receiveResult

    val step1 = initStep match {
      case Cont(next) => next(data, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Cont instead of $state")
    }

    val step2 = step1 match {
      case Emit(_: ServerDataBlock, next) =>
        val Cont(afterNext) = next(data, emptyBuf)
        afterNext(data, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Emit Data Block instead of $state")
    }

    val step3 = step2 match {
      case Emit(_: ProfileInfo, next) =>
        val Cont(afterNext) = next(data, emptyBuf)
        afterNext(data, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Emit Profile Info instead of $state")
    }

    val step4 = step3 match {
      case Emit(_: Progress, next) =>
        val Cont(afterNext) = next(data, emptyBuf)
        afterNext(data, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Emit Progress instead of $state")
    }

    val step5 = step4 match {
      case Emit(_: ServerDataBlock, next) =>
        val Cont(afterNext) = next(data, emptyBuf)
        afterNext(data, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Emit Data Block instead of $state")
    }

    assertEquals(step5, Done)
  }

  test("receiveResult not enough data") {
    val emptyBuf = ByteBuffer.allocate(0)

    val step1 = ProtocolSteps.receiveResult match {
      case Cont(next) => next(emptyBuf, emptyBuf)
      case state => throw new IllegalStateException(s"Shall be in step Cont instead of $state")
    }

    assertEquals(step1.getClass, classOf[NeedsInput])
  }
}