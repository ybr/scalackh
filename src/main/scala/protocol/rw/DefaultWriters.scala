package scalackh.protocol.rw

import java.nio.ByteBuffer

import scalackh.protocol.rw.LEB128.writeVarInt

object DefaultWriters {
  def writeBool(z: Boolean, buf: ByteBuffer): Unit = {
    val byte: Byte = if(z) 1 else 0
    buf.put(byte)
    ()
  }

  def writeInt(n: Int, buf: ByteBuffer): Unit = {
    buf.putInt(n)
    ()
  }

  def writeString(str: String, buf: ByteBuffer): Unit = writeBytes(str.getBytes("UTF-8"), buf)

  def writeBytes(bytes: Array[Byte], buf: ByteBuffer): Unit = {
    writeVarInt(bytes.length, buf)
    buf.put(bytes)
    ()
  }
}