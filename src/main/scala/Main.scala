import java.net.Socket
import java.io._
import java.nio.ByteBuffer

import utils._
import ckh.native._

object Main {
  def main(args: Array[String]): Unit = {
    val DEBUG = true

    val BUFFER_SIZE: Int = 4096 //1048576

    println("Connecting...")

    val socket = new Socket("127.0.0.1", 32769)
    val (is, os) = {
      if(DEBUG) {
        (new DebugInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE)),
          new BufferedOutputStream(new DebugOutputStream(socket.getOutputStream()), BUFFER_SIZE))
      }
      else {
        (new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE),
          new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE))
      }
    }

    println("Connected")

    val bb = ByteBuffer.allocate(BUFFER_SIZE)

    ClientPacketWriters.message.write(ClientInfo(
      "scala client",
      Version(19, 1, 6),
      "default",
      "default",
      ""
    ), bb)

    val a = bb.array()
    write(bb, os)
    os.flush()
    bb.clear()

    println("Waiting for incoming data...")

    var querySent = false

    var i: Int = 0
    val bytes = new Array[Byte](BUFFER_SIZE)
    while(i < 20) {
      val n = is.available()
      if(n > 0) {
        is.read(bytes, 0, n)
        val readBB = ByteBuffer.wrap(bytes, 0, n)
        while(readBB.hasRemaining()) {
          ServerPacketReaders.protocol.read(readBB) match {
            case ServerBlock(block) =>
              block.columns.map {
                case DateColumn(name, data) => println(name + " " + data.toList)
                case DateTimeColumn(name, data) => println(name + " " + data.toList)
                case Float32Column(name, data) => println(name + " " + data.toList)
                case Float64Column(name, data) => println(name + " " + data.toList)
                case Int8Column(name, data) => println(name + " " + data.toList)
                case Int16Column(name, data) => println(name + " " + data.toList)
                case Int32Column(name, data) => println(name + " " + data.toList)
                case Int64Column(name, data) => println(name + " " + data.toList)
                case StringColumn(name, data) => println(name + " " + data.toList)
              }
            case _ => ()
          }
        }

        if(!querySent) {
          ClientPacketWriters.message.write(Query(
            None,
            Complete,
            None,
            "select * from toto"
          ), bb)

          ClientPacketWriters.message.write(ClientBlock(Block.empty), bb)

          // ClientPacketWriters.message.write(Ping, bb)

          write(bb, os)
          os.flush()
          bb.clear()

          querySent = true
        }
      }
      else {
        Thread.sleep(10)
        println(".")
      }
      i = i + 1
    }

    socket.close()
  }

  def write(buffer: ByteBuffer, os: OutputStream): Unit = {
    os.write(buffer.array, 0, buffer.position)
  }
}