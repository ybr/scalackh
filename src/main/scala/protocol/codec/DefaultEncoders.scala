package scalackh.protocol.codec

import java.nio.ByteBuffer

import scalackh.protocol.codec.LEB128.writeVarInt

object DefaultEncoders {
  def writeBool(z: Boolean, buf: ByteBuffer): Unit = {
    val byte: Byte = if(z) 1 else 0
    buf.put(byte)
    ()
  }

  def writeString(str: String, buf: ByteBuffer): Unit = writeBytes(str.getBytes("UTF-8"), buf)

  def writeBytes(bytes: Array[Byte], buf: ByteBuffer): Unit = {
    writeVarInt(bytes.length, buf)
    buf.put(bytes)
    ()
  }
}