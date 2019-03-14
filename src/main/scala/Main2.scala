package ckh

import java.net.Socket
import java.io._
import java.nio.ByteBuffer
import java.time._

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

    // val values = Iterator(Block(
    //     table = None,
    //     BlockInfo.empty,
    //     nbColumns = 1,
    //     nbRows = 1,
    //     columns = List(DateColumn("date", Array(LocalDate.now.plusDays(20))))
    // ))

    val ext = Iterator(Block(
      Some("coucou"),
      BlockInfo.empty,
      1,
      1,
      List(
        Float64Column("ahah", Array(1.56))
      )
    ))

    // connection.insert("INSERT INTO toto1 VALUES", values)


    val blocks = connection.query("SELECT x1 FROM special2").toList

    blocks.zipWithIndex.foreach { case (block, i) =>
      println(block.columns(0))
      // println(s"Block $i " + block.columns(0).asInstanceOf[NullableColumn].nulls.toList)
      println(s"Block $i " + (block.columns(0) match {
        case DateColumn(_, data) => data.toList
        case DateTimeColumn(_, data) => data.toList
        case EnumColumn(_, _, enums, data) => data.toList.map(enums)
        case FixedStringColumn(_, _, data) => data.toList
        case Float32Column(_, data) => data.toList
        case Float64Column(_, data) => data.toList
        case Int8Column(_, data) => data.toList
        case Int16Column(_, data) => data.toList
        case Int32Column(_, data) => data.toList
        case Int64Column(_, data) => data.toList
        case NullableColumn(_, data, nulls) => data.toList
        case StringColumn(_, data) => data.toList
        case UuidColumn(_, data) => data.toList
      }))
    }
    connection.disconnect()
  }
}