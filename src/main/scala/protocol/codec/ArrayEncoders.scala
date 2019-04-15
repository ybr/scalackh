package scalackh.protocol.codec

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import scalackh.protocol._
import scalackh.protocol.codec.DefaultEncoders.writeString

object ArrayEncoders {
  val arrayEncoder: Encoder[ClickhouseArray] = Encoder { (array, buf) =>
    array match {
      case DateArray(dates) =>
        arraySizesEncoder.write(sizesFromArray(dates), buf)
        arrayDateEncoder.write(dates, buf)
      case DateTimeArray(times) =>
        arraySizesEncoder.write(sizesFromArray(times), buf)
        arrayDateTimeEncoder.write(times, buf)
      case Float32Array(floats) =>
        arraySizesEncoder.write(sizesFromArray(floats), buf)
        arrayFloat32Encoder.write(floats, buf)
      case Float64Array(doubles) =>
        arraySizesEncoder.write(sizesFromArray(doubles), buf)
        arrayFloat64Encoder.write(doubles, buf)
      case Int8Array(ints) =>
        arraySizesEncoder.write(sizesFromArray(ints), buf)
        arrayByteEncoder.write(ints, buf)
      case Int16Array(ints) =>
        arraySizesEncoder.write(sizesFromArray(ints), buf)
        arrayShortEncoder.write(ints, buf)
      case Int32Array(ints) =>
        arraySizesEncoder.write(sizesFromArray(ints), buf)
        arrayIntEncoder.write(ints, buf)
      case Int64Array(ints) =>
        arraySizesEncoder.write(sizesFromArray(ints), buf)
        arrayLongEncoder.write(ints, buf)
      case StringArray(strings) =>
        arraySizesEncoder.write(sizesFromArray(strings), buf)
        arrayStringEncoder.write(strings, buf)
      case UuidArray(uuids) =>
        arraySizesEncoder.write(sizesFromArray(uuids), buf)
        arrayUuidEncoder.write(uuids, buf)
      case UInt8Array(uints) =>
        arraySizesEncoder.write(sizesFromArray(uints), buf)
        arrayByteEncoder.write(uints, buf)
      case UInt16Array(uints) =>
        arraySizesEncoder.write(sizesFromArray(uints), buf)
        arrayShortEncoder.write(uints, buf)
      case UInt32Array(uints) =>
        arraySizesEncoder.write(sizesFromArray(uints), buf)
        arrayIntEncoder.write(uints, buf)
      case UInt64Array(uints) =>
        arraySizesEncoder.write(sizesFromArray(uints), buf)
        arrayLongEncoder.write(uints, buf)
    }
  }

  // see doc in ArrayDecoders
  def sizesFromArray[T](array: Array[Array[T]]): Array[Int] = {
    val sizes: Array[Int] = new Array[Int](array.length)

    var i: Int = 0
    var offset: Int = 0
    while(i < array.length) {
      offset = offset + array(i).length
      sizes(i) = offset
      i = i + 1
    }

    sizes
  }

  // manages only 1-d arrays
  val arraySizesEncoder: Encoder[Array[Int]] = Encoder { (array, buf) =>
    array.foreach(int => buf.putLong(int.toLong))
  }

  val arrayDateEncoder: Encoder[Array[Array[LocalDate]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putShort(inArray(j).toEpochDay().toShort)
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayDateTimeEncoder: Encoder[Array[Array[LocalDateTime]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putInt(inArray(j).toEpochSecond(ZoneOffset.UTC).toInt)
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayFloat32Encoder: Encoder[Array[Array[Float]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putFloat(inArray(j))
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayFloat64Encoder: Encoder[Array[Array[Double]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putDouble(inArray(j))
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayByteEncoder: Encoder[Array[Array[Byte]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      buf.put(arrays(i))
      i = i + 1
    }
  }

  val arrayShortEncoder: Encoder[Array[Array[Short]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putShort(inArray(j))
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayIntEncoder: Encoder[Array[Array[Int]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putInt(inArray(j))
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayLongEncoder: Encoder[Array[Array[Long]]] = Encoder { (arrays, buf) =>
    var i: Int = 0
    while(i < arrays.length) {
      val inArray = arrays(i)
      var j: Int = 0
      while(j < inArray.length) {
        buf.putLong(inArray(j))
        j = j + 1
      }
      i = i + 1
    }
  }

  val arrayStringEncoder: Encoder[Array[Array[String]]] = Encoder { (strings, buf) =>
    strings.foreach(_.foreach(writeString(_, buf)))
  }

  val arrayUuidEncoder: Encoder[Array[Array[UUID]]] = Encoder { (uuids, buf) =>
    uuids.foreach(_.foreach { uuid =>
      buf.putLong(uuid.getMostSignificantBits())
      buf.putLong(uuid.getLeastSignificantBits())
    })
  }
}