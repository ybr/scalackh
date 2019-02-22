import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import ckh.native._
import DefaultReaders._

object ColumnReaders {
  def dateColumnReader(name: String, size: Int): Reader[DateColumn] = Reader { buf =>
    val data: Array[LocalDate] = new Array[LocalDate](size)

    var i: Int = 0
    while(i < size) {
      data(i) = LocalDate.ofEpochDay(readShort(buf))
      i = i + 1
    }

    DateColumn(name, data)
  }

  def datetimeColumnReader(name: String, size: Int): Reader[DateTimeColumn] = Reader { buf =>
    val data: Array[LocalDateTime] = new Array[LocalDateTime](size)

    var i: Int = 0
    while(i < size) {
      data(i) = LocalDateTime.ofEpochSecond(readInt(buf), 0, ZoneOffset.UTC)
      i = i + 1
    }

    DateTimeColumn(name, data)
  }

  def float32ColumnReader(name: String, size: Int): Reader[Float32Column] = Reader { buf =>
    val data: Array[Float] = new Array[Float](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readFloat(buf)
      i = i + 1
    }

    Float32Column(name, data)
  }

  def float64ColumnReader(name: String, size: Int): Reader[Float64Column] = Reader { buf =>
    val data: Array[Double] = new Array[Double](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readDouble(buf)
      i = i + 1
    }

    Float64Column(name, data)
  }

  def int8ColumnReader(name: String, size: Int): Reader[Int8Column] = Reader { buf =>
    val data: Array[Byte] = new Array[Byte](size)

    var i: Int = 0
    while(i < size) {
      data(i) = buf.get()
      i = i + 1
    }

    Int8Column(name, data)
  }

  def int16ColumnReader(name: String, size: Int): Reader[Int16Column] = Reader { buf =>
    val data: Array[Short] = new Array[Short](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readShort(buf)
      i = i + 1
    }

    Int16Column(name, data)
  }

  def int32ColumnReader(name: String, size: Int): Reader[Int32Column] = Reader { buf =>
    val data: Array[Int] = new Array[Int](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readInt(buf)
      i = i + 1
    }

    Int32Column(name, data)
  }

  def int64ColumnReader(name: String, size: Int): Reader[Int64Column] = Reader { buf =>
    val data: Array[Long] = new Array[Long](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readLong(buf)
      i = i + 1
    }

    Int64Column(name, data)
  }

  def stringColumnReader(name: String, size: Int): Reader[StringColumn] = Reader { buf =>
    val data: Array[String] = new Array[String](size)

    var i: Int = 0
    while(i < size) {
      data(i) = readString(buf)
      i = i + 1
    }

    StringColumn(name, data)
  }
}