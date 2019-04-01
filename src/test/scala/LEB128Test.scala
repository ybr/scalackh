package scalackh.protocol.rw

import java.nio.ByteBuffer

import org.scalacheck._
import org.scalacheck.Prop.forAll

object LEB128Test extends Properties("LEB128") {

  property("writeVarInt/varIntReader") = forAll(Gen.choose(0, Int.MaxValue)) { (n1: Int) =>
    val buf = ByteBuffer.allocate(5)
    LEB128.writeVarInt(n1, buf)
    buf.rewind()
    val Consumed(n2) = LEB128.varIntReader.read(buf)

    n1 == n2
  }
}