package scalackh.protocol.codec

import java.nio.ByteBuffer
import java.time.ZoneOffset

import scalackh.protocol._
import scalackh.protocol.codec.DefaultEncoders._

object ColumnDataEncoders {
  val dateColumnDataEncoder: Encoder[DateColumnData] = Encoder { (col, buf) =>
    writeString("Date", buf)

    col.data.foreach { date =>
      writeShort(date.toEpochDay().toShort, buf)
    }
  }

  val datetimeColumnDataEncoder: Encoder[DateTimeColumnData] = Encoder { (col, buf) =>
    writeString("DateTime", buf)

    col.data.foreach { datetime =>
      writeInt(datetime.toEpochSecond(ZoneOffset.UTC).toInt, buf)
    }
  }

  // val enum8ColumnDataEncoder: Encoder[Enum8ColumnData] = Encoder { (col, buf) =>
  //   val enumDefs: String = col.enums.toList.sortBy(_._1).map(t => t._1 + " = " + t._2).mkString(",")
  //   writeString(s"Enum8($enumDefs)", buf)
  //   col.data.foreach { e =>
  //     val b: Byte = (e & 0xff).toByte
  //     buf.put(b)
  //   }
  // }

  val float32ColumnDataEncoder: Encoder[Float32ColumnData] = Encoder { (col, buf) =>
    writeString("Float32", buf)
    col.data.foreach(writeFloat(_, buf))
  }

  val float64ColumnDataEncoder: Encoder[Float64ColumnData] = Encoder { (col, buf) =>
    writeString("Float64", buf)
    col.data.foreach(writeDouble(_, buf))
  }

  val int8ColumnDataEncoder: Encoder[Int8ColumnData] = Encoder { (col, buf) =>
    writeString("Int8", buf)
    col.data.foreach(buf.put)
  }

  val int16ColumnDataEncoder: Encoder[Int16ColumnData] = Encoder { (col, buf) =>
    writeString("Int16", buf)
    col.data.foreach(writeShort(_, buf))
  }

  val int32ColumnDataEncoder: Encoder[Int32ColumnData] = Encoder { (col, buf) =>
    writeString("Int32", buf)
    col.data.foreach(writeInt(_, buf))
  }

  val int64ColumnDataEncoder: Encoder[Int64ColumnData] = Encoder { (col, buf) =>
    writeString("Int64", buf)
    col.data.foreach(writeLong(_, buf))
  }

  val stringColumnDataEncoder: Encoder[StringColumnData] = Encoder { (col, buf) =>
    writeString("String", buf)
    col.data.foreach(writeString(_, buf))
  }

  def writeShort(s: Short, buf: ByteBuffer): Unit = {
    buf.putShort(s)
    ()
  }

  def writeLong(n: Long, buf: ByteBuffer): Unit = {
    buf.putLong(n)
    ()
  }

  def writeDouble(n: Double, buf: ByteBuffer): Unit = {
    buf.putDouble(n)
    ()
  }

  def writeFloat(n: Float, buf: ByteBuffer): Unit = {
    buf.putFloat(n)
    ()
  }
}