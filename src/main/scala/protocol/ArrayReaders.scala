package ckh.protocol

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import ckh.native._
import DefaultReaders._

import scala.collection.immutable.IntMap

object ArrayReaders {
  def arrayReader(nbRows: Int, elemType: String): Reader[ClickhouseArray] = Reader { buf =>
    val sizes = arraySizesReader(nbRows).read(buf)

    elemType match {
      case "Date" => arrayDateReader(nbRows, sizes).read(buf)
      case "DateTime" => arrayDateTimeReader(nbRows, sizes).read(buf)
      case "Float32" => arrayFloatReader(nbRows, sizes).read(buf)
      case "Float64" => arrayDoubleReader(nbRows, sizes).read(buf)
      case "Int8" => arrayByteReader(nbRows, sizes).read(buf)
      case "Int16" => arrayShortReader(nbRows, sizes).read(buf)
      case "Int32" => arrayIntReader(nbRows, sizes).read(buf)
      case "Int64" => arrayLongReader(nbRows, sizes).read(buf)
      case "String" => arrayStringReader(nbRows, sizes).read(buf)
    }
  }

  def arraySizesReader(nbRows: Int): Reader[Array[Int]] = Reader { buf =>
    // From https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/columns/arraycolumn.py
    // Nested arrays written in flatten form after information about their
    // sizes (offsets really).
    // One element of array of arrays can be represented as tree:
    // (0 depth)          [[3, 4], [5, 6]]
    //                   |               |
    // (1 depth)      [3, 4]           [5, 6]
    //                |    |           |    |
    // (leaf)        3     4          5     6
    // Offsets (sizes) written in breadth-first search order. In example above
    // following sequence of offset will be written: 4 -> 2 -> 4
    // 1) size of whole array: 4
    // 2) size of array 1 in depth=1: 2
    // 3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4
    // After sizes info comes flatten data: 3 -> 4 -> 5 -> 6

    val sizes: Array[Int] = new Array[Int](nbRows)

    var i: Int = 0
    var currentOffset: Int = 0

    while(i < nbRows) {
      val nextOffset = readLong(buf).toInt
      sizes(i) = nextOffset - currentOffset
      currentOffset = nextOffset
      i = i + 1
    }

    sizes
  }

  def arrayDateReader(nbRows: Int, sizes: Array[Int]): Reader[DateArray] = Reader { buf =>
    val data: Array[Array[LocalDate]] = new Array[Array[LocalDate]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[LocalDate](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = LocalDate.ofEpochDay(readShort(buf))
        j = j + 1
      }

      i = i + 1
    }

    DateArray(data)
  }

  def arrayDateTimeReader(nbRows: Int, sizes: Array[Int]): Reader[DateTimeArray] = Reader { buf =>
    val data: Array[Array[LocalDateTime]] = new Array[Array[LocalDateTime]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[LocalDateTime](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = LocalDateTime.ofEpochSecond(readInt(buf), 0, ZoneOffset.UTC)
        j = j + 1
      }

      i = i + 1
    }

    DateTimeArray(data)
  }

  def arrayByteReader(nbRows: Int, sizes: Array[Int]): Reader[ByteArray] = Reader { buf =>
    val data: Array[Array[Byte]] = new Array[Array[Byte]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Byte](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = buf.get()
        j = j + 1
      }

      i = i + 1
    }

    ByteArray(data)
  }

  def arrayDoubleReader(nbRows: Int, sizes: Array[Int]): Reader[DoubleArray] = Reader { buf =>
    val data: Array[Array[Double]] = new Array[Array[Double]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Double](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readDouble(buf)
        j = j + 1
      }

      i = i + 1
    }

    DoubleArray(data)
  }

  def arrayFloatReader(nbRows: Int, sizes: Array[Int]): Reader[FloatArray] = Reader { buf =>
    val data: Array[Array[Float]] = new Array[Array[Float]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Float](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readFloat(buf)
        j = j + 1
      }

      i = i + 1
    }

    FloatArray(data)
  }

  def arrayIntReader(nbRows: Int, sizes: Array[Int]): Reader[IntArray] = Reader { buf =>
    val data: Array[Array[Int]] = new Array[Array[Int]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Int](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readInt(buf)
        j = j + 1
      }

      i = i + 1
    }

    IntArray(data)
  }

  def arrayLongReader(nbRows: Int, sizes: Array[Int]): Reader[LongArray] = Reader { buf =>
    val data: Array[Array[Long]] = new Array[Array[Long]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Long](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readLong(buf)
        j = j + 1
      }

      i = i + 1
    }

    LongArray(data)
  }

  def arrayShortReader(nbRows: Int, sizes: Array[Int]): Reader[ShortArray] = Reader { buf =>
    val data: Array[Array[Short]] = new Array[Array[Short]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[Short](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readShort(buf)
        j = j + 1
      }

      i = i + 1
    }

    ShortArray(data)
  }

  def arrayStringReader(nbRows: Int, sizes: Array[Int]): Reader[StringArray] = Reader { buf =>
    val data: Array[Array[String]] = new Array[Array[String]](nbRows)

    var i: Int = 0
    while(i < nbRows) {
      val arraySize: Int = sizes(i)
      val arr = new Array[String](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize) {
        arr(j) = readString(buf)
        j = j + 1
      }

      i = i + 1
    }

    StringArray(data)
  }
}