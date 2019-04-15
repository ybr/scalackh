package scalackh.protocol.codec

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import scalackh.protocol._
import scalackh.protocol.codec.DefaultDecoders.stringDecoder

object ArrayDecoders {
  def arrayDecoder(nbRows: Int, elemType: String): Decoder[ClickhouseArray] = for {
    sizes <- arraySizesDecoder(nbRows)
    array <- elemType match {
      case "Date" => arrayDateDecoder(sizes)
      case "DateTime" => arrayDateTimeDecoder(sizes)
      case "Float32" => arrayFloat32Decoder(sizes)
      case "Float64" => arrayFloat64Decoder(sizes)
      case "Int8" => arrayByteDecoder(sizes).map(Int8Array)
      case "Int16" => arrayShortDecoder(sizes).map(Int16Array)
      case "Int32" => arrayIntDecoder(sizes).map(Int32Array)
      case "Int64" => arrayLongDecoder(sizes).map(Int64Array)
      case "String" => arrayStringDecoder(sizes)
      case "UInt8" => arrayByteDecoder(sizes).map(UInt8Array)
      case "UInt16" => arrayShortDecoder(sizes).map(UInt16Array)
      case "UInt32" => arrayIntDecoder(sizes).map(UInt32Array)
      case "UInt64" => arrayLongDecoder(sizes).map(UInt64Array)
      case "UUID" => arrayUuidDecoder(sizes)
    }
  } yield array

  // manages only 1-d arrays
  def arraySizesDecoder(nbRows: Int): Decoder[Array[Int]] = Decoder { buf =>
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

    if(buf.remaining < nbRows * 8) NotEnough
    else {
      val sizes: Array[Int] = new Array[Int](nbRows)

      var i: Int = 0
      var currentOffset: Int = 0

      while(i < nbRows) {
        val nextOffset = buf.getLong().toInt
        sizes(i) = nextOffset - currentOffset
        currentOffset = nextOffset
        i = i + 1
      }

      Consumed(sizes)
    }
  }

  def arrayDateDecoder(sizes: Array[Int]): Decoder[DateArray] = Decoder { buf =>
    if(buf.remaining < sizes.length * 2) NotEnough
    else {
      val data: Array[Array[LocalDate]] = new Array[Array[LocalDate]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[LocalDate](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = LocalDate.ofEpochDay(buf.getShort().toLong)
          j = j + 1
        }

        i = i + 1
      }

      Consumed(DateArray(data))
    }
  }

  def arrayDateTimeDecoder(sizes: Array[Int]): Decoder[DateTimeArray] = Decoder { buf =>
    if(buf.remaining < sizes.length * 4) NotEnough
    else {
      val data: Array[Array[LocalDateTime]] = new Array[Array[LocalDateTime]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[LocalDateTime](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = LocalDateTime.ofEpochSecond(buf.getInt().toLong, 0, ZoneOffset.UTC)
          j = j + 1
        }

        i = i + 1
      }

      Consumed(DateTimeArray(data))
    }
  }

  def arrayFloat32Decoder(sizes: Array[Int]): Decoder[Float32Array] = Decoder { buf =>
    if(buf.remaining < sizes.length * 4) NotEnough
    else {
      val data: Array[Array[Float]] = new Array[Array[Float]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[Float](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = buf.getFloat()
          j = j + 1
        }

        i = i + 1
      }

      Consumed(Float32Array(data))
    }
  }

  def arrayFloat64Decoder(sizes: Array[Int]): Decoder[Float64Array] = Decoder { buf =>
    if(buf.remaining < sizes.length * 8) NotEnough
    else {
      val data: Array[Array[Double]] = new Array[Array[Double]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[Double](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = buf.getDouble()
          j = j + 1
        }

        i = i + 1
      }

      Consumed(Float64Array(data))
    }
  }

  def arrayByteDecoder(sizes: Array[Int]): Decoder[Array[Array[Byte]]] = Decoder { buf =>
    if(buf.remaining < sizes.length * 8) NotEnough
    else {
      val data: Array[Array[Byte]] = new Array[Array[Byte]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
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

      Consumed(data)
    }
  }

  def arrayShortDecoder(sizes: Array[Int]): Decoder[Array[Array[Short]]] = Decoder { buf =>
    if(buf.remaining < sizes.length * 8) NotEnough
    else {
      val data: Array[Array[Short]] = new Array[Array[Short]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[Short](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = buf.getShort()
          j = j + 1
        }

        i = i + 1
      }

      Consumed(data)
    }
  }

  def arrayIntDecoder(sizes: Array[Int]): Decoder[Array[Array[Int]]] = Decoder { buf =>
    if(buf.remaining < sizes.length * 8) NotEnough
    else {
      val data: Array[Array[Int]] = new Array[Array[Int]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[Int](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = buf.getInt()
          j = j + 1
        }

        i = i + 1
      }

      Consumed(data)
    }
  }

  def arrayLongDecoder(sizes: Array[Int]): Decoder[Array[Array[Long]]] = Decoder { buf =>
    if(buf.remaining < sizes.length * 8) NotEnough
    else {
      val data: Array[Array[Long]] = new Array[Array[Long]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[Long](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          arr(j) = buf.getLong()
          j = j + 1
        }

        i = i + 1
      }

      Consumed(data)
    }
  }

  def arrayStringDecoder(sizes: Array[Int]): Decoder[StringArray] = Decoder { buf =>
    val data: Array[Array[String]] = new Array[Array[String]](sizes.length)

    var hasEnough: Boolean = true
    var i: Int = 0
    while(i < sizes.length && hasEnough) {
      val arraySize: Int = sizes(i)
      val arr = new Array[String](arraySize)
      data(i) = arr

      var j: Int = 0
      while(j < arraySize && hasEnough) {
        stringDecoder.read(buf) match {
          case Consumed(str) => arr(j) = str
          case NotEnough => hasEnough = false
        }
        j = j + 1
      }

      i = i + 1
    }

    if(!hasEnough) NotEnough
    else Consumed(StringArray(data))
  }

  def arrayUuidDecoder(sizes: Array[Int]): Decoder[UuidArray] = Decoder { buf =>
    if(buf.remaining < sizes.length * 16) NotEnough
    else {
      val data: Array[Array[UUID]] = new Array[Array[UUID]](sizes.length)

      var i: Int = 0
      while(i < sizes.length) {
        val arraySize: Int = sizes(i)
        val arr = new Array[UUID](arraySize)
        data(i) = arr

        var j: Int = 0
        while(j < arraySize) {
          val mostSigBits: Long = buf.getLong()
          val leastSigBits: Long = buf.getLong()
          arr(j) = new UUID(mostSigBits, leastSigBits)
          j = j + 1
        }

        i = i + 1
      }

      Consumed(UuidArray(data))
    }
  }
}