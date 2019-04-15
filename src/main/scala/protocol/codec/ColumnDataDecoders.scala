package scalackh.protocol.codec

import java.util.UUID

import scala.collection.immutable.IntMap

import scalackh.protocol._
import scalackh.protocol.codec.DefaultDecoders.stringDecoder

object ColumnDataDecoders {
  val nullable = "Nullable\\((.+)\\)".r
  val fixedString = "FixedString\\(([0-9]+)\\)".r
  val enum8 = "Enum8\\((.+)\\)".r
  val enum16 = "Enum16\\((.+)\\)".r
  val enumDef = "'(.+)' = ([0-9]+)".r
  val array = "Array\\((.+)\\)".r

  def columnDataDecoder(nbRows: Int): Decoder[ColumnData] = for {
    columnType <- stringDecoder
    columnData <- columnDataOnlyDecoder(nbRows, columnType)
  } yield columnData

  def columnDataOnlyDecoder(nbRows: Int, columnType: String): Decoder[ColumnData] = {
    columnType match {
      case array(elemType) => arrayColumnDataDecoder(nbRows, elemType)
      case "Date" => dateColumnDataDecoder(nbRows)
      case "DateTime" => dateTimeColumnDataDecoder(nbRows)
      case enum8(enumStr) =>
        val enums: Map[Int, String] = enumsFromDef(enumStr)
        enum8ColumnDataDecoder(enums, nbRows)
      case enum16(enumStr) =>
        val enums: Map[Int, String] = enumsFromDef(enumStr)
        enum16ColumnDataDecoder(enums, nbRows)
      case fixedString(strLength) => fixedStringColumnDataDecoder(strLength.toInt, nbRows)
      case "Float32" => float32ColumnDataDecoder(nbRows)
      case "Float64" => float64ColumnDataDecoder(nbRows)
      case "Int8" => int8ColumnDataDecoder(nbRows)
      case "Int16" => int16ColumnDataDecoder(nbRows)
      case "Int32" => int32ColumnDataDecoder(nbRows)
      case "Int64" => int64ColumnDataDecoder(nbRows)
      case nullable(nullableType) => nullableColumnDataDecoder(nbRows, columnDataOnlyDecoder(nbRows, nullableType))
      case "String" => stringColumnDataDecoder(nbRows)
      case "UInt8" => uint8ColumnDataDecoder(nbRows)
      case "UInt16" => uint16ColumnDataDecoder(nbRows)
      case "UInt32" => uint32ColumnDataDecoder(nbRows)
      case "UInt64" => uint64ColumnDataDecoder(nbRows)
      case "UUID" => uuidColumnDataDecoder(nbRows)
      case other => throw new UnsupportedOperationException(s"Column type not supported ${other}")
    }
  }

  def enumsFromDef(enumStr: String): Map[Int, String] = IntMap[String](enumStr.split(',').toSeq.map(_.trim).map {
    case enumDef(key, value) => (value.toInt, key)
  }: _*)

  def arrayColumnDataDecoder(nbRows: Int, elemType: String): Decoder[ArrayColumnData] = {
    ArrayDecoders.arrayDecoder(nbRows, elemType).map { arrays =>
      ArrayColumnData(arrays)
    }
  }

  def dateColumnDataDecoder(nbRows: Int): Decoder[DateColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 2) NotEnough
    else {
      val data: Array[Short] = new Array[Short](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getShort()
        i = i + 1
      }

      Consumed(DateColumnData(data))
    }
  }

  def dateTimeColumnDataDecoder(nbRows: Int): Decoder[DateTimeColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 4) NotEnough
    else {
      val data: Array[Int] = new Array[Int](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getInt()
        i = i + 1
      }

      Consumed(DateTimeColumnData(data))
    }
  }

  def enum8ColumnDataDecoder(enums: Map[Int, String], nbRows: Int): Decoder[EnumColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows) NotEnough
    else {
      val data: Array[Byte] = new Array[Byte](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.get()
        i = i + 1
      }

      Consumed(Enum8ColumnData(enums, data))
    }
  }

  def enum16ColumnDataDecoder(enums: Map[Int, String], nbRows: Int): Decoder[EnumColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 2) NotEnough
    else {
      val data: Array[Short] = new Array[Short](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getShort()
        i = i + 1
      }

      Consumed(Enum16ColumnData(enums, data))
    }
  } 

  def fixedStringColumnDataDecoder(strLength: Int, nbRows: Int): Decoder[FixedStringColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * strLength) NotEnough
    else {
      val data: Array[String] = new Array[String](nbRows)

      var i: Int = 0
      val bytes: Array[Byte] = new Array[Byte](strLength)
      while(i < nbRows) {
        buf.get(bytes)
        data(i) =  new String(bytes, "UTF-8")
        i = i + 1
      }

      Consumed(FixedStringColumnData(strLength, data))
    }
  }

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

  def nullableArrayDecoder(nbRows: Int): Decoder[Array[Boolean]] = Decoder { buf =>
    if(buf.remaining < nbRows) NotEnough
    else {
      val nulls: Array[Boolean] = new Array[Boolean](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        nulls(i) = buf.get() != 0
        i = i + 1
      }

      Consumed(nulls)
    }
  }

  def nullableColumnDataDecoder(nbRows: Int, decoder: Decoder[ColumnData]): Decoder[NullableColumnData] = for {
    nulls <- nullableArrayDecoder(nbRows)
    data <- decoder
  } yield NullableColumnData(nulls, data)

  def stringColumnDataDecoder(nbRows: Int): Decoder[StringColumnData] = Decoder { buf =>
    val data: Array[String] = new Array[String](nbRows)

    var hasEnough: Boolean = true
    var i: Int = 0

    while(i < nbRows && hasEnough) {
      DefaultDecoders.stringDecoder.read(buf) match {
        case Consumed(str) => data(i) = str
        case NotEnough => hasEnough = false
      }
      i = i + 1
    }

    if(hasEnough) Consumed(StringColumnData(data))
    else NotEnough
  }

  def uint8ColumnDataDecoder(nbRows: Int): Decoder[UInt8ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows) NotEnough
    else {
      val data: Array[Byte] = new Array[Byte](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.get()
        i = i + 1
      }

      Consumed(UInt8ColumnData(data))
    }
  }

  def uint16ColumnDataDecoder(nbRows: Int): Decoder[UInt16ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 2) NotEnough
    else {
      val data: Array[Short] = new Array[Short](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getShort()
        i = i + 1
      }

      Consumed(UInt16ColumnData(data))
    }
  }

  def uint32ColumnDataDecoder(nbRows: Int): Decoder[UInt32ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 4) NotEnough
    else {
      val data: Array[Int] = new Array[Int](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getInt()
        i = i + 1
      }

      Consumed(UInt32ColumnData(data))
    }
  }

  def uint64ColumnDataDecoder(nbRows: Int): Decoder[UInt64ColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 8) NotEnough
    else {
      val data: Array[Long] = new Array[Long](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        data(i) = buf.getLong()
        i = i + 1
      }

      Consumed(UInt64ColumnData(data))
    }
  }

  def uuidColumnDataDecoder(nbRows: Int): Decoder[UuidColumnData] = Decoder { buf =>
    if(buf.remaining < nbRows * 16) NotEnough
    else {
      val data: Array[UUID] = new Array[UUID](nbRows)

      var i: Int = 0
      while(i < nbRows) {
        val mostSigBits: Long = buf.getLong()
        val leastSigBits: Long = buf.getLong()
        data(i) = new UUID(mostSigBits, leastSigBits)
        i = i + 1
      }

      Consumed(UuidColumnData(data))
    }
  }
}