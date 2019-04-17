package scalackh.protocol.codec

import java.nio.ByteBuffer

import org.scalacheck._
import org.scalacheck.Prop.forAll

object LEB128Test extends Properties("LEB128") {

  property("writeVarInt/varIntDecoder") = forAll(Gen.choose(0, Int.MaxValue)) { (n1: Int) =>
    val buf = ByteBuffer.allocate(128)
    LEB128.writeVarInt(n1, buf)
    buf.rewind()
    val Consumed(n2) = LEB128.varIntDecoder.read(buf)

    n1 == n2
  }

  property("writeVarLong/varLongDecoder") = forAll(Gen.choose(0, Long.MaxValue).filter(_ >= 0)) { (n1: Long) =>
      val buf = ByteBuffer.allocate(128)
      LEB128.writeVarLong(n1, buf)
      buf.rewind()
      val Consumed(n2) = LEB128.varLongDecoder.read(buf)

      n1 == n2
  }
}