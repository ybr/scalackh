package utils

import java.io.{FilterInputStream, InputStream}

class DumpPacketInputStream(in: InputStream) extends FilterInputStream(in) {
  override def read(b: Array[Byte]): Int = {
    val nbBytes: Int = in.read(b)
    println(s"<<<<<<<<<< Received ${nbBytes} bytes")
    println(HexDump.format(b, 0, nbBytes))
    nbBytes
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val nbBytes: Int = in.read(b, off, len)
    println(s"<<<<<<<<<< Received ${nbBytes} bytes")
    println(HexDump.format(b, off, nbBytes))
    nbBytes
  }
}