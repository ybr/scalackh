package scalackh.protocol.rw

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scalackh.protocol._

object ColumnDataReaders {
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

  def columnDataReader(nbRows: Int, columnType: String): Reader[ColumnData] = Reader { buf =>
    columnType match {
      // case array(elemType) => arrayColumnDataReader(nbRows, elemType).read(buf)
      case "Date" => dateColumnDataReader(nbRows).read(buf)
      case "DateTime" => datetimeColumnDataReader(nbRows).read(buf)
      // case enum8(enumStr) =>
      //   val enums: Map[Int, String] = enumsFromDef(enumStr)
      //   enum8ColumnDataReader(enums, nbRows).read(buf)
      // case enum16(enumStr) =>
      //   val enums: Map[Int, String] = enumsFromDef(enumStr)
      //   enum16ColumnDataReader(enums, nbRows).read(buf)
      // case fixedString(strLength) => fixedStringColumnDataReader(strLength.toInt, nbRows).read(buf)
      case "Float32" => float32ColumnDataReader(nbRows).read(buf)
      case "Float64" => float64ColumnDataReader(nbRows).read(buf)
      case "Int8" => int8ColumnDataReader(nbRows).read(buf)
      case "Int16" => int16ColumnDataReader(nbRows).read(buf)
      case "Int32" => int32ColumnDataReader(nbRows).read(buf)
      case "Int64" => int64ColumnDataReader(nbRows).read(buf)
      // case nullable(nullableType) => nullableColumnDataReader(nbRows, columnDataReader(nbRows, nullableType)).read(buf)
      // case "String" => stringColumnDataReader(nbRows).read(buf)
      // case tuple(types) => tupleColumnDataReader(nbRows, types).read(buf)
      // case "UInt8" => uint8ColumnDataReader(nbRows).read(buf)
      // case "UInt16" => uint16ColumnDataReader(nbRows).read(buf)
      // case "UInt32" => uint32ColumnDataReader(nbRows).read(buf)
      // case "UInt64" => uint64ColumnDataReader(nbRows).read(buf)
      // case "UUID" => uuidColumnDataReader(nbRows).read(buf)

      case other => throw new UnsupportedOperationException(s"Column type not supported ${other}")
    }
  }

  // def arrayColumnDataReader(nbRows: Int, elemType: String): Reader[ArrayColumnData] = Reader { buf =>
  //   val arrays = arrayReader(nbRows, elemType).read(buf)

  //   ArrayColumnData(arrays)
  // }

  def dateColumnDataReader(nbRows: Int): Reader[DateColumnData] = Reader { buf =>
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

  def datetimeColumnDataReader(nbRows: Int): Reader[DateTimeColumnData] = Reader { buf =>
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

  // def enum8ColumnDataReader(enums: Map[Int, String], nbRows: Int): Reader[EnumColumnData] = Reader { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = buf.get().toInt
  //     i = i + 1
  //   }

  //   Enum8ColumnData(enums, data)
  // }

  // def enum16ColumnDataReader(enums: Map[Int, String], nbRows: Int): Reader[EnumColumnData] = Reader { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = readShort(buf).toInt
  //     i = i + 1
  //   }

  //   Enum16ColumnData(enums, data)
  // }

  // def fixedStringColumnDataReader(strLength: Int, nbRows: Int): Reader[FixedStringColumnData] = Reader { buf =>
  //   val data: Array[String] = new Array[String](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = readStringFixed(strLength, buf)
  //     i = i + 1
  //   }

  //   FixedStringColumnData(strLength, data)
  // }

  def float32ColumnDataReader(nbRows: Int): Reader[Float32ColumnData] = Reader { buf =>
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

  def float64ColumnDataReader(nbRows: Int): Reader[Float64ColumnData] = Reader { buf =>
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

  def int8ColumnDataReader(nbRows: Int): Reader[Int8ColumnData] = Reader { buf =>
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

  def int16ColumnDataReader(nbRows: Int): Reader[Int16ColumnData] = Reader { buf =>
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

  def int32ColumnDataReader(nbRows: Int): Reader[Int32ColumnData] = Reader { buf =>
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

  def int64ColumnDataReader(nbRows: Int): Reader[Int64ColumnData] = Reader { buf =>
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

  // def nullableColumnDataReader(nbRows: Int, reader: Reader[ColumnData]): Reader[NullableColumnData] = Reader { buf =>
  //   val nulls: Array[Boolean] = new Array[Boolean](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     nulls(i) = readBool(buf)
  //     i = i + 1
  //   }

  //   NullableColumnData(nulls, reader.read(buf))
  // }

  // def stringColumnDataReader(nbRows: Int): Reader[StringColumnData] = Reader { buf =>
  //   val data: Array[String] = new Array[String](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = readString(buf)
  //     i = i + 1
  //   }

  //   StringColumnData(data)
  // }

  // def tupleColumnDataReader(nbRows: Int, typesStr: String): Reader[TupleColumnData] = Reader { buf =>
  //   val reader = tupleReader(typesStr)

  //   val data: Array[TupleData] = new Array[TupleData](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = reader.read(buf)
  //     i = i + 1
  //   }

  //   TupleColumnData(data)
  // }

  // def uint8ColumnDataReader(nbRows: Int): Reader[UInt8ColumnData] = Reader { buf =>
  //   val data: Array[Short] = new Array[Short](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = (0xff & buf.get()).toShort
  //     i = i + 1
  //   }

  //   UInt8ColumnData(data)
  // }

  // def uint16ColumnDataReader(nbRows: Int): Reader[UInt16ColumnData] = Reader { buf =>
  //   val data: Array[Int] = new Array[Int](nbRows)

  //   var i: Int = 0
  //   while(i < nbRows) {
  //     data(i) = (0xffff & readShort(buf)).toInt
  //     i = i + 1
  //   }

  //   UInt16ColumnData(data)
  // }

  // def uint32ColumnDataReader(nbRows: Int): Reader[UInt32ColumnData] = Reader { buf =>
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

  // def uint64ColumnDataReader(nbRows: Int): Reader[UInt64ColumnData] = Reader { buf =>
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

  // def uuidColumnDataReader(nbRows: Int): Reader[UuidColumnData] = Reader { buf =>
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