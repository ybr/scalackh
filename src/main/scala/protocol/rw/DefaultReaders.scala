package scalackh.protocol.rw

import java.nio.{ByteBuffer, ByteOrder}

import scalackh.protocol.rw.LEB128.readVarInt

object DefaultReaders {
  def readBool(buf: ByteBuffer): Boolean = buf.get() != 0

  def readBytesFixed(length: Int, buf: ByteBuffer): Array[Byte] = {
    val bytesFixedLength = new Array[Byte](length)
    buf.get(bytesFixedLength)
    bytesFixedLength
  }

  def readBytes(buf: ByteBuffer): Array[Byte] = {
    val length: Int = readVarInt(buf)
    readBytesFixed(length, buf)
  }

  def readStringFixed(length: Int, buf: ByteBuffer): String = new String(readBytesFixed(length, buf), "UTF-8")

  def readString(buf: ByteBuffer): String = new String(readBytes(buf), "UTF-8")

  def readShort(buf: ByteBuffer): Short = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getShort()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readInt(buf: ByteBuffer): Int = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val int = buf.getInt()
    buf.order(ByteOrder.BIG_ENDIAN)
    int
  }

  def readLong(buf: ByteBuffer): Long = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getLong()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readFloat(buf: ByteBuffer): Float = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getFloat()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readDouble(buf: ByteBuffer): Double = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getDouble()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }
}