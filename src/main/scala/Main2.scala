package ckh

import java.net.Socket
import java.io._
import java.nio.ByteBuffer
import java.time.LocalDate

import utils._
import ckh.native._

class ClickhouseClientException(message: String) extends RuntimeException(message)
class ClickhouseServerException(causal: ServerException) extends RuntimeException {
  override def toString(): String = causal.toString()
}

object Main {
  def main(args: Array[String]): Unit = {
    val client = Client("127.0.0.1", 32781)
    val connection = client.connect()
    val blocks = connection.query("SELECT date, f32, sum(f64) FROM toto GROUP BY date, f32").toList

    println(blocks)
    println(blocks(0).columns(0).asInstanceOf[DateColumn].data.toList)
    println(blocks(0).columns(1).asInstanceOf[Float32Column].data.toList)
    println(blocks(0).columns(2).asInstanceOf[Float64Column].data.toList)
    connection.disconnect()
  }
}