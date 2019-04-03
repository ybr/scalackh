package scalackh.protocol.codec

import scalackh.protocol._

import java.nio.{ByteBuffer, ByteOrder}

import org.scalacheck._
import org.scalacheck.Prop.forAll

object ColumnDataCodecTest extends Properties("ColumnData codec") {
  property("string") = forAll(Gen.listOfN(10, Gen.alphaNumStr)) { (str1: List[String]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.stringColumnDataEncoder.write(StringColumnData(str1.toArray), buf)
    buf.position(0)
    val Consumed(StringColumnData(str2)) = ColumnDataDecoders.stringColumnDataDecoder(str1.length).read(buf)

    str1 == str2.toList
  }
}