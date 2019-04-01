package scalackh.protocol.steps

import java.nio.ByteBuffer

import scalackh.protocol._

sealed trait ProtocolStep
case class Cont(next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case class Emit(packet: ServerPacket, next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case class NeedsInput(next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case object Done extends ProtocolStep
case class Error(msg: String) extends ProtocolStep

object Cont {
  def apply(next: => ProtocolStep): ProtocolStep = Cont { (_, _) =>
    next
  }

  def i(next: ByteBuffer => ProtocolStep): ProtocolStep = Cont { (i, _) =>
    next(i)
  }

  def o(next: ByteBuffer => ProtocolStep): ProtocolStep = Cont { (_, o) =>
    next(o)
  }

  def io(next: (ByteBuffer, ByteBuffer) => ProtocolStep): ProtocolStep = Cont { (i, o) =>
    next(i, o)
  }
}

object NeedsInput {
  def apply(next: => ProtocolStep): ProtocolStep = NeedsInput((_, _) => next)

  def i(next: ByteBuffer => ProtocolStep): ProtocolStep = NeedsInput { (i, _) =>
    next(i)
  }

  def o(next: ByteBuffer => ProtocolStep): ProtocolStep = NeedsInput { (_, o) =>
    next(o)
  }

  def io(next: (ByteBuffer, ByteBuffer) => ProtocolStep): ProtocolStep = NeedsInput { (i, o) =>
    next(i, o)
  }
}

object Emit {
  def apply(packet: ServerPacket, next: => ProtocolStep): ProtocolStep = Emit(packet, (_, _) => next)
}