package ckh.protocol

import java.nio.{ByteBuffer, ByteOrder}

import LEB128._

object DefaultReaders {
  def readBool(buf: ByteBuffer): Boolean = buf.get() != 0

  def readBytes(bytes: ByteBuffer): Array[Byte] = {
    val length: Int = readVarInt(bytes)
    val bytesFixedLength = new Array[Byte](length)
    bytes.get(bytesFixedLength)
    bytesFixedLength
  }

  def readString(bytes: ByteBuffer): String = new String(readBytes(bytes), "UTF-8")

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