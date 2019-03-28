package scalackh.client.utils

import java.io.{FilterOutputStream, OutputStream}

class DumpPacketOutputStream(out: OutputStream) extends FilterOutputStream(out) {
  override def write(b: Array[Byte]): Unit = {
    println(s">>>>>>>>>> Sending ${b.length} bytes")
    println(HexDump.format(b, 0, b.length))
    out.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    println(s">>>>>>>>>> Sending ${len} bytes")
    println(HexDump.format(b, off, len))
    out.write(b, off, len)
  }
}