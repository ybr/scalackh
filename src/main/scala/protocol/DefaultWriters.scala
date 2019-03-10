package ckh.protocol

import java.nio.{ByteBuffer, ByteOrder}

import LEB128._

object DefaultWriters {
  def writeBool(z: Boolean, buf: ByteBuffer): Unit = {
    val byte: Byte = if(z) 1 else 0
    buf.put(byte)
  }

  def writeShort(s: Short, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(s)
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  def writeInt(n: Int, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(n)
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  def writeString(str: String, buf: ByteBuffer): Unit = writeBytes(str.getBytes("UTF-8"), buf)

  def writeBytes(bytes: Array[Byte], buf: ByteBuffer): Unit = {
    writeVarInt(bytes.length, buf)
    buf.put(bytes)
  }
}