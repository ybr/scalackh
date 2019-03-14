package ckh.protocol

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import ckh.native._
import DefaultReaders._

import scala.collection.immutable.IntMap

object ColumnReaders {
  val nullable = "Nullable\\((.+)\\)".r
  val fixedString = "FixedString\\(([0-9]+)\\)".r
  val enum = "Enum([0-9]+)\\((.+)\\)".r
  val enumDef = "'(.+)' = ([0-9]+)".r

  def columnReader(name: String, nbRows: Int, columnType: String): Reader[Column] = Reader { buf =>
    columnType match {
      case "Date" => dateColumnReader(name, nbRows).read(buf)
      case "DateTime" => datetimeColumnReader(name, nbRows).read(buf)
      case enum(bits, enumStr) =>
        val enums: Map[Int, String] = IntMap[String](enumStr.split(',').toSeq.map(_.trim).map {
          case enumDef(key, value) => (value.toInt, key)
        }: _*)
        enumColumnReader(name, bits.toInt, enums, nbRows).read(buf)
      case fixedString(strLength) => fixedStringColumnReader(name, strLength.toInt, nbRows).read(buf)
      case "Float32" => float32ColumnReader(name, nbRows).read(buf)
      case "Float64" => float64ColumnReader(name, nbRows).read(buf)
      case "Int8" => int8ColumnReader(name, nbRows).read(buf)
      case "Int16" => int16ColumnReader(name, nbRows).read(buf)
      case "Int32" => int32ColumnReader(name, nbRows).read(buf)
      case "Int64" => int64ColumnReader(name, nbRows).read(buf)
      case nullable(nullableType) => nullableColumnReader(name, nbRows, columnReader(name, nbRows, nullableType)).read(buf)
      case "String" => stringColumnReader(name, nbRows).read(buf)
      case "UUID" => uuidColumnReader(name, nbRows).read(buf)
    }
  }

  def dateColumnReader(name: String, nbRows: Int): Reader[DateColumn] = Reader { buf =>
    val data: Array[LocalDate] = new Array[LocalDate](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = LocalDate.ofEpochDay(readShort(buf))
      i = i + 1
    }

    DateColumn(name, data)
  }

  def datetimeColumnReader(name: String, nbRows: Int): Reader[DateTimeColumn] = Reader { buf =>
    val data: Array[LocalDateTime] = new Array[LocalDateTime](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = LocalDateTime.ofEpochSecond(readInt(buf), 0, ZoneOffset.UTC)
      i = i + 1
    }

    DateTimeColumn(name, data)
  }

  def enumColumnReader(name: String, bits: Int, enums: Map[Int, String], nbRows: Int): Reader[EnumColumn] = Reader { buf =>
    val data: Array[Int] = new Array[Int](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = {
        if(bits == 8) buf.get()
        else readShort(buf)
      }
      i = i + 1
    }

    EnumColumn(name, bits, enums, data)
  }

  def fixedStringColumnReader(name: String, strLength: Int, nbRows: Int): Reader[FixedStringColumn] = Reader { buf =>
    val data: Array[String] = new Array[String](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readStringFixed(strLength, buf)
      i = i + 1
    }

    FixedStringColumn(name, strLength, data)
  }

  def float32ColumnReader(name: String, nbRows: Int): Reader[Float32Column] = Reader { buf =>
    val data: Array[Float] = new Array[Float](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readFloat(buf)
      i = i + 1
    }

    Float32Column(name, data)
  }

  def float64ColumnReader(name: String, nbRows: Int): Reader[Float64Column] = Reader { buf =>
    val data: Array[Double] = new Array[Double](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readDouble(buf)
      i = i + 1
    }

    Float64Column(name, data)
  }

  def int8ColumnReader(name: String, nbRows: Int): Reader[Int8Column] = Reader { buf =>
    val data: Array[Byte] = new Array[Byte](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = buf.get()
      i = i + 1
    }

    Int8Column(name, data)
  }

  def int16ColumnReader(name: String, nbRows: Int): Reader[Int16Column] = Reader { buf =>
    val data: Array[Short] = new Array[Short](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readShort(buf)
      i = i + 1
    }

    Int16Column(name, data)
  }

  def int32ColumnReader(name: String, nbRows: Int): Reader[Int32Column] = Reader { buf =>
    val data: Array[Int] = new Array[Int](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readInt(buf)
      i = i + 1
    }

    Int32Column(name, data)
  }

  def int64ColumnReader(name: String, nbRows: Int): Reader[Int64Column] = Reader { buf =>
    val data: Array[Long] = new Array[Long](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readLong(buf)
      i = i + 1
    }

    Int64Column(name, data)
  }

  def nullableColumnReader(name: String, nbRows: Int, reader: Reader[Column]): Reader[NullableColumn] = Reader { buf =>
    val nulls: Array[Boolean] = new Array[Boolean](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      nulls(i) = readBool(buf)
      i = i + 1
    }

    NullableColumn(name, nulls, reader.read(buf))
  }

  def stringColumnReader(name: String, nbRows: Int): Reader[StringColumn] = Reader { buf =>
    val data: Array[String] = new Array[String](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      data(i) = readString(buf)
      i = i + 1
    }

    StringColumn(name, data)
  }

  def uuidColumnReader(name: String, nbRows: Int): Reader[UuidColumn] = Reader { buf =>
    val data: Array[UUID] = new Array[UUID](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val mostSigBits: Long = readLong(buf)
      val leastSigBits: Long = readLong(buf)
      data(i) = new UUID(mostSigBits, leastSigBits)
      i = i + 1
    }

    UuidColumn(name, data)
  }
}