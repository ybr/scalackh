package ckh

import ckh.native._
import ckh.protocol._

import java.nio.ByteBuffer

sealed trait ProtocolStep
case class Cont(next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case class Emit(packet: ServerPacket, next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case class NeedsInput(next: (ByteBuffer, ByteBuffer) => ProtocolStep) extends ProtocolStep
case object Done extends ProtocolStep
case class Error(msg: String) extends ProtocolStep

object Cont {
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

import ckh.native._

object ProtocolSteps {
  def  sendHello(info: ClientInfo): ProtocolStep = Cont.o { buf =>
    ClientPacketWriters.message.write(info, buf)
    receiveHello
  }

  val receiveHello: ProtocolStep = NeedsInput.i { buf =>
    ServerPacketReaders.protocol.read(buf) match {
      case info: ServerInfo =>
        // println("Received server info: " + info)
        Emit(info, (_, _) => Done)
      case other => Error("Unexpected packet: " + other)
    }
  }

  def query(q: String): ProtocolStep = Cont.o { buf =>
    ClientPacketWriters.message.write(Query(None, Complete, None, q), buf)
    ClientPacketWriters.message.write(ClientDataBlock(Block.empty), buf) // external tables
    receiveResult
  }

  val receiveResult: ProtocolStep = multipacket(NeedsInput.i { buf =>
    val packet = ServerPacketReaders.protocol.read(buf)
    packet match {
      case p: ServerInfo => Error("Unexpected packet: " + p)
      case p: ServerDataBlock => Emit(p, (_, _) => receiveResult)
      case p: Progress => Emit(p, (_, _) => receiveResult)
      case p: ProfileInfo => Emit(p, (_, _) => receiveResult)
      case p: TotalsBlock => Emit(p, (_, _) => receiveResult)
      case p: ExtremesBlock => Emit(p, (_, _) => receiveResult)
      case p: LogBlock => Emit(p, (_, _) => receiveResult)
      case p: ServerException => Emit(p, (_, _) => Done)
      case Pong => Emit(Pong, (_, _) => receiveResult)
      case EndOfStream => Done
    }
  })

  // val receiveResult: ProtocolStep = multipacket(receiveResultLoop)

  def multipacket(step: ProtocolStep): ProtocolStep = step match {
    case NeedsInput(next) => Cont { (i, o) =>
      if(i.position < i.limit) multipacket(next(i, o))
      else NeedsInput.i(_ => multipacket(NeedsInput(next)))
    }
    case other => other
  }
}