package ckh.protocol

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import ckh.native._
import DefaultWriters._

object ColumnWriters {
  val dateColumnWriter: Writer[DateColumn] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Date", buf)

    col.data.foreach { date =>
      writeShort(date.toEpochDay().toShort, buf)
    }
  }

  val datetimeColumnWriter: Writer[DateTimeColumn] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("DateTime", buf)

    col.data.foreach { datetime =>
      writeInt(datetime.toEpochSecond(ZoneOffset.UTC).toInt, buf)
    }
  }

  val float32ColumnWriter: Writer[Float32Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Float32", buf)
    col.data.foreach(writeFloat(_, buf))
  }

  val float64ColumnWriter: Writer[Float64Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Float64", buf)
    col.data.foreach(writeDouble(_, buf))
  }

  val int8ColumnWriter: Writer[Int8Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Int8", buf)
    col.data.foreach(buf.put)
  }

  val int16ColumnWriter: Writer[Int16Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Int16", buf)
    col.data.foreach(writeShort(_, buf))
  }

  val int32ColumnWriter: Writer[Int32Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Int32", buf)
    col.data.foreach(writeInt(_, buf))
  }

  val int64ColumnWriter: Writer[Int64Column] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("Int64", buf)
    col.data.foreach(writeLong(_, buf))
  }

  val stringColumnWriter: Writer[StringColumn] = Writer { (col, buf) =>
    writeString(col.name, buf)
    writeString("String", buf)
    col.data.foreach(writeString(_, buf))
  }

  def writeShort(s: Short, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(s)
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  def writeLong(n: Long, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(n)
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  def writeDouble(n: Double, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(n)
    buf.order(ByteOrder.BIG_ENDIAN)
  }

  def writeFloat(n: Float, buf: ByteBuffer): Unit = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putFloat(n)
    buf.order(ByteOrder.BIG_ENDIAN)
  }
}