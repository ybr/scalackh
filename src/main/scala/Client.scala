package ckh

import ckh.native._
import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.net.{InetAddress, Socket}
import java.nio.ByteBuffer
import utils._

class Client(
  val address: InetAddress,
  val port: Int,
  val user: Option[String] = None,
  val password: Option[String] = None) {

  def connect(): Connection = {
    val socket = new Socket(address, port)

    val is = new BufferedInputStream(new DumpPacketInputStream(socket.getInputStream()), Client.BUFFER_SIZE)
    val os = new BufferedOutputStream(new DumpPacketOutputStream(socket.getOutputStream()), Client.BUFFER_SIZE)

    // val is = new BufferedInputStream(socket.getInputStream(), Client.BUFFER_SIZE)
    // val os = new BufferedOutputStream(socket.getOutputStream(), Client.BUFFER_SIZE)

    val in = ByteBuffer.allocate(Client.BUFFER_SIZE)
    val out = ByteBuffer.allocate(Client.BUFFER_SIZE)

    val serverInfo: ServerInfo = Connection.iterator(is, os, in, out, ProtocolSteps.sendHello(ClientInfo(
      Client.name,
      Client.version,
      user.getOrElse("default"),
      password.getOrElse("default"),
      ""
    )))
    .collect { case Emit(si: ServerInfo, _) => si }
    .next

    Connection(socket, is, os, in , out, serverInfo)
  }
}

object Client {
  val version = Version(19, 1, 6)
  val name = "scala-client"

  val BUFFER_SIZE: Int = 4096 //1048576

  def apply(
    host: String,
    port: Int,
    user: Option[String] = None,
    password: Option[String] = None
  ): Client = new Client(InetAddress.getByName(host), port, user, password)
}

case class Connection(socket: Socket, is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, serverInfo: ServerInfo) {

  def query(sql: String, externalTables: Iterator[Block] = Iterator.empty): Iterator[Block] = {
    Connection.iterator(is, os, in, out, ProtocolSteps.execute(sql, externalTables, Iterator.empty))
    .collect {
      case Emit(ServerDataBlock(block), _) if(block.nbColumns > 0 && block.nbRows > 0) => block
      case Emit(x: ServerException, _) => throw new ClickhouseServerException(x)
    }
  }

  def insert(sql: String, values: Iterator[Block]): Unit = {
    Connection.iterator(is, os, in, out, ProtocolSteps.execute(sql, Iterator.empty, values))
    .foreach(_ => ())
  }

  def disconnect(): Unit = socket.close()
}

object Connection {
  def iterator(is: InputStream, os: OutputStream, in: ByteBuffer, out: ByteBuffer, step: ProtocolStep): Iterator[ProtocolStep] = new Iterator[ProtocolStep] {
    var currentStep: ProtocolStep = step
    var done: Boolean = false

    def hasNext(): Boolean = !done

    def next(): ProtocolStep = {
      if(done) throw new IllegalStateException("Stream exhausted")
      else {
        currentStep match {
          case Cont(step) =>
            val nextStep = step(in, out)
            if(out.position > 0) {
              os.write(out.array, 0, out.position)
              os.flush()
              out.clear()
            }
            currentStep = nextStep

          case NeedsInput(step) =>
            in.clear()

            // wait for input
            val n = is.read(in.array(), 0, in.limit())
            in.position(n)
            in.flip()

            currentStep = step(in, out)

          case e: Emit =>
            currentStep = Cont(e.next)
            e
          case Done =>
            done = true
            currentStep = Done
          case Error(msg) =>
            done = true
            currentStep = Done
            throw new ClickhouseClientException(msg)
        }

        currentStep
      }
    }
  }
}