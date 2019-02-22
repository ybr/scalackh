package utils

import java.io.{FilterInputStream, InputStream}

class DebugInputStream(in: InputStream) extends FilterInputStream(in) {
  override def read(b: Array[Byte]): Int = {
    val nbBytes: Int = in.read(b)
    println(s"<<<<<<<<<< Received ${nbBytes} bytes")
    println(HexDump.format(b, 0, b.length))
    nbBytes
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val nbBytes: Int = in.read(b, off, len)
    println(s"<<<<<<<<<< Received ${nbBytes} bytes")
    println(HexDump.format(b, off, len))
    nbBytes
  }
}