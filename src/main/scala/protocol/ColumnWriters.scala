package ckh.protocol

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

  // def datetimeColumnWriter(name: String, size: Int): Writer[DateTimeColumn] = Writer { buf =>
  //   val data: Array[LocalDateTime] = new Array[LocalDateTime](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = LocalDateTime.ofEpochSecond(readInt(buf), 0, ZoneOffset.UTC)
  //     i = i + 1
  //   }

  //   DateTimeColumn(name, data)
  // }

  // def float32ColumnWriter(name: String, size: Int): Writer[Float32Column] = Writer { buf =>
  //   val data: Array[Float] = new Array[Float](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readFloat(buf)
  //     i = i + 1
  //   }

  //   Float32Column(name, data)
  // }

  // def float64ColumnWriter(name: String, size: Int): Writer[Float64Column] = Writer { buf =>
  //   val data: Array[Double] = new Array[Double](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readDouble(buf)
  //     i = i + 1
  //   }

  //   Float64Column(name, data)
  // }

  // def int8ColumnWriter(name: String, size: Int): Writer[Int8Column] = Writer { buf =>
  //   val data: Array[Byte] = new Array[Byte](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = buf.get()
  //     i = i + 1
  //   }

  //   Int8Column(name, data)
  // }

  // def int16ColumnWriter(name: String, size: Int): Writer[Int16Column] = Writer { buf =>
  //   val data: Array[Short] = new Array[Short](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readShort(buf)
  //     i = i + 1
  //   }

  //   Int16Column(name, data)
  // }

  // def int32ColumnWriter(name: String, size: Int): Writer[Int32Column] = Writer { buf =>
  //   val data: Array[Int] = new Array[Int](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readInt(buf)
  //     i = i + 1
  //   }

  //   Int32Column(name, data)
  // }

  // def int64ColumnWriter(name: String, size: Int): Writer[Int64Column] = Writer { buf =>
  //   val data: Array[Long] = new Array[Long](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readLong(buf)
  //     i = i + 1
  //   }

  //   Int64Column(name, data)
  // }

  // def stringColumnWriter(name: String, size: Int): Writer[StringColumn] = Writer { buf =>
  //   val data: Array[String] = new Array[String](size)

  //   var i: Int = 0
  //   while(i < size) {
  //     data(i) = readString(buf)
  //     i = i + 1
  //   }

  //   StringColumn(name, data)
  // }
}