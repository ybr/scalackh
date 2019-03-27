package scalackh.protocol.rw

import java.nio.ByteBuffer

// Encode integer of variable length using LEB128
// https://en.wikipedia.org/wiki/LEB128
object LEB128 {
  def readVarInt(buf: ByteBuffer): Int = {
    var result: Int = 0
    var byte: Int = 0
    var shift: Int = 0
    do {
      byte = buf.get().toInt
      result |= (byte & 0x7f) << shift
      shift += 7
    } while((byte & 0x80) != 0)

    result
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