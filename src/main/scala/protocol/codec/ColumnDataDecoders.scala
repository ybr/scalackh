package scalackh.protocol.codec

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scalackh.protocol._

object ColumnDataDecoders {
  // val nullable = "Nullable\\((.+)\\)".r
  // val fixedString = "FixedString\\(([0-9]+)\\)".r
  // val enum8 = "Enum8\\((.+)\\)".r
  // val enum16 = "Enum16\\((.+)\\)".r
  // val enumDef = "'(.+)' = ([0-9]+)".r
  // val array = "Array\\((.+)\\)".r
  // val tuple = "Tuple\\((.+)\\)".r

  // def enumsFromDef(enumStr: String): Map[Int, String] = IntMap[String](enumStr.split(',').toSeq.map(_.trim).map {
  //   case enumDef(key, value) => (value.toInt, key)
  // }: _*)

  def columnDataDecoder(nbRows: Int, columnType: String): Decoder[ColumnData] = {
    columnType match {
      // case array(elemType) => arrayColumnDataDecoder(nbRows, elemType).read(buf)
      case "Date" => dateColumnDataDecoder(nbRows)
      case "DateTime" => datetimeColumnDataDecoder(nbRows)
      // case enum8(enumStr) =>
      //   val enums: Map[Int, String] = enumsFromDef(enumStr)
      //   enum8ColumnDataDecoder(enums, nbRows)
      // case enum16(enumStr) =>
      //   val enums: Map[Int, String] = enumsFromDef(enumStr)
      //   enum16ColumnDataDecoder(enums, nbRows)
      // case fixedString(strLength) => fixedStringColumnDataDecoder(strLength.toInt, nbRows)
      case "Float32" => float32ColumnDataDecoder(nbRows)
      case "Float64" => float64ColumnDataDecoder(nbRows)
      case "Int8" => int8ColumnDataDecoder(nbRows)
      case "Int16" => int16ColumnDataDecoder(nbRows)
      case "Int32" => int32ColumnDataDecoder(nbRows)
      case "Int64" => int64ColumnDataDecoder(nbRows)
      // case nullable(nullableType) => nullableColumnDataDecoder(nbRows, columnDataDecoder(nbRows, nullableType))
      case "String" => stringColumnDataDecoder(nbRows)
      // case tuple(types) => tupleColumnDataDecoder(nbRows, types)
      // case "UInt8" => uint8ColumnDataDecoder(nbRows)
      // case "UInt16" => uint16ColumnDataDecoder(nbRows)
      // case "UInt32" => uint32ColumnDataDecoder(nbRows)
      // case "UInt64" => uint64ColumnDataDecoder(nbRows)
      // case "UUID" => uuidColumnDataDecoder(nbRows)

      case other => throw new UnsupportedOperationException(s"Column type not supported ${other}")
    }
  }

  // def arrayColumnDataDecoder(nbRows: Int, elemType: String): Decoder[ArrayColumnData] = Decoder { buf =>
  //   val arrays = arrayDecoder(nbRows, elemType)

  //   ArrayColumnData(arrays)
  // }

  def dateColumnDataDecoder(nbRows: Int): Decoder[DateColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 2) NotEnough
    else {
      val data: Array[LocalDate] = new Array[LocalDate](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = LocalDate.ofEpochDay(buf.getShort().toLong)
        i = i + 1
      }

      Consumed(DateColumnData(data))
    }
  }

  def datetimeColumnDataDecoder(nbRows: Int): Decoder[DateTimeColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 4) NotEnough
    else {
      val data: Array[LocalDateTime] = new Array[LocalDateTime](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = LocalDateTime.ofEpochSecond(buf.getInt().toLong, 0, ZoneOffset.UTC)
        i = i + 1
      }

      Consumed(DateTimeColumnData(data))
    }
  }

  // def enum8ColumnDataDecoder(enums: Map[Int, String], nbRows: Int): Decoder[EnumColumnData] = Decoder { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = buf.get().toInt
  //     i = i + 1
  //   }

  //   Enum8ColumnData(enums, data)
  // }

  // def enum16ColumnDataDecoder(enums: Map[Int, String], nbRows: Int): Decoder[EnumColumnData] = Decoder { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = readShort(buf).toInt
  //     i = i + 1
  //   }

  //   Enum16ColumnData(enums, data)
  // }

  // def fixedStringColumnDataDecoder(strLength: Int, nbRows: Int): Decoder[FixedStringColumnData] = Decoder { buf =>
  //   val data: Array[String] = new Array[String](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = readStringFixed(strLength, buf)
  //     i = i + 1
  //   }

  //   FixedStringColumnData(strLength, data)
  // }

  def float32ColumnDataDecoder(nbRows: Int): Decoder[Float32ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 4) NotEnough
    else {
      val data: Array[Float] = new Array[Float](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getFloat()
        i = i + 1
      }

      Consumed(Float32ColumnData(data))
    }
  }

  def float64ColumnDataDecoder(nbRows: Int): Decoder[Float64ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 8) NotEnough
    else {
      val data: Array[Double] = new Array[Double](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getDouble()
        i = i + 1
      }

      Consumed(Float64ColumnData(data))
    }
  }

  def int8ColumnDataDecoder(nbRows: Int): Decoder[Int8ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows) NotEnough
    else {
      val data: Array[Byte] = new Array[Byte](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.get()
        i = i + 1
      }

      Consumed(Int8ColumnData(data))
    }
  }

  def int16ColumnDataDecoder(nbRows: Int): Decoder[Int16ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 2) NotEnough
    else {
      val data: Array[Short] = new Array[Short](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getShort()
        i = i + 1
      }

      Consumed(Int16ColumnData(data))
    }
  }

  def int32ColumnDataDecoder(nbRows: Int): Decoder[Int32ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 4) NotEnough
    else {
      val data: Array[Int] = new Array[Int](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getInt()
        i = i + 1
      }

      Consumed(Int32ColumnData(data))
    }
  }

  def int64ColumnDataDecoder(nbRows: Int): Decoder[Int64ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 8) NotEnough
    else {
      val data: Array[Long] = new Array[Long](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getLong()
        i = i + 1
      }

      Consumed(Int64ColumnData(data))
    }
  }

  // def nullableColumnDataDecoder(nbRows: Int, Decoder: Decoder[ColumnData]): Decoder[NullableColumnData] = Decoder { buf =>
  //   val nulls: Array[Boolean] = new Array[Boolean](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     nulls(i) = readBool(buf)
  //     i = i + 1
  //   }

  //   NullableColumnData(nulls, Decoder.read(buf))
  // }

  def stringColumnDataDecoder(nbRows: Int): Decoder[StringColumnData] = Decoder { buf =>
    val data: Array[String] = new Array[String](nbRows)

    var hasEnough: Boolean = true
    var i: Int = 0

    while(i < nbRows) {
      DefaultDecoders.stringDecoder.read(buf) match {
        case Consumed(str) => data(i) = str
        case NotEnough => hasEnough = false
      }
      i = i + 1
    }

    if(hasEnough) Consumed(StringColumnData(data))
    else NotEnough
  }

  // def tupleColumnDataDecoder(nbRows: Int, typesStr: String): Decoder[TupleColumnData] = Decoder { buf =>
  //   val Decoder = tupleDecoder(typesStr)

  //   val data: Array[TupleData] = new Array[TupleData](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = Decoder.read(buf)
  //     i = i + 1
  //   }

  //   TupleColumnData(data)
  // }

  // def uint8ColumnDataDecoder(nbRows: Int): Decoder[UInt8ColumnData] = Decoder { buf =>
  //   val data: Array[Short] = new Array[Short](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = (0xff & buf.get()).toShort
  //     i = i + 1
  //   }

  //   UInt8ColumnData(data)
  // }

  // def uint16ColumnDataDecoder(nbRows: Int): Decoder[UInt16ColumnData] = Decoder { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = (0xffff & readShort(buf)).toInt
  //     i = i + 1
  //   }

  //   UInt16ColumnData(data)
  // }

  // def uint32ColumnDataDecoder(nbRows: Int): Decoder[UInt32ColumnData] = Decoder { buf =>
  //   val data: Array[Long] = new Array[Long](nbRows)

  //   val bytes: Array[Byte] = new Array(4)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     buf.get(bytes)

  //     var result: Long = (bytes(3) & 0xff).toLong
  //     result = (result << 8) + (bytes(2) & 0xff)
  //     result = (result << 8) + (bytes(1) & 0xff)
  //     result = (result << 8) + (bytes(0) & 0xff)

  //     data(i) = result
  //     i = i + 1
  //   }

  //   UInt32ColumnData(data)
  // }

  // def uint64ColumnDataDecoder(nbRows: Int): Decoder[UInt64ColumnData] = Decoder { buf =>
  //   val data: Array[BigInteger] = new Array[BigInteger](nbRows)

  //   val bytes: Array[Byte] = new Array(8)

  //   var i: Int = 0
  //   while(i < nbRows) {

  //     bytes(7) = buf.get()
  //     bytes(6) = buf.get()
  //     bytes(5) = buf.get()
  //     bytes(4) = buf.get()
  //     bytes(3) = buf.get()
  //     bytes(2) = buf.get()
  //     bytes(1) = buf.get()
  //     bytes(0) = buf.get()

  //     data(i) = new BigInteger(bytes)
  //     i = i + 1
  //   }

  //   UInt64ColumnData(data)
  // }

  // def uuidColumnDataDecoder(nbRows: Int): Decoder[UuidColumnData] = Decoder { buf =>
  //   val data: Array[UUID] = new Array[UUID](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     val mostSigBits: Long = readLong(buf)
  //     val leastSigBits: Long = readLong(buf)
  //     data(i) = new UUID(mostSigBits, leastSigBits)
  //     i = i + 1
  //   }

  //   UuidColumnData(data)
  // }
}