package scalackh.protocol.codec

import java.nio.ByteBuffer

// Encode integer of variable length using LEB128
// https://en.wikipedia.org/wiki/LEB128
object LEB128 {
  val varIntDecoder: Decoder[Int] = Decoder { buf =>
    var hasEnough: Boolean = true
    var result: Int = 0
    var byte: Int = 0
    var shift: Int = 0

    do {
      if(buf.remaining >= 1) {
        byte = buf.get().toInt
        result |= (byte & 0x7f) << shift
        shift += 7
      }
      else {
        hasEnough = false
      }
    } while((byte & 0x80) != 0 && hasEnough)

    if(hasEnough) Consumed(result)
    else NotEnough
  }

  def writeVarInt(n: Int, buf: ByteBuffer): Unit = {
    var value: Int = n
    do {
      var byte: Int = value & 0x7f // low order 7 bites of value
      value = value >> 7
      if(value != 0) byte = byte | 0x80  // more bytes to come set high order bit of byte
      buf.put(byte.toByte)
    } while(value != 0)
  }
}