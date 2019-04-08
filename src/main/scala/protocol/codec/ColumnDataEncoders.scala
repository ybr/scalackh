package scalackh.protocol.codec

import java.time.ZoneOffset

import scalackh.protocol._
import scalackh.protocol.codec.DefaultEncoders.writeString

object ColumnDataEncoders {
  val dateColumnDataEncoder: Encoder[DateColumnData] = Encoder { (col, buf) =>
    col.data.foreach { date =>
      buf.putShort(date.toEpochDay().toShort)
    }
  }

  val dateTimeColumnDataEncoder: Encoder[DateTimeColumnData] = Encoder { (col, buf) =>
    col.data.foreach { datetime =>
      buf.putInt(datetime.toEpochSecond(ZoneOffset.UTC).toInt)
    }
  }

  val enum8ColumnDataEncoder: Encoder[Enum8ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.put)
  }

  val enum16ColumnDataEncoder: Encoder[Enum16ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putShort)
  }

  val fixedStringColumnDataEncoder: Encoder[FixedStringColumnData] = Encoder { (col, buf) =>
    col.data.foreach(str => buf.put(str.getBytes("UTF-8")))
  }

  val float32ColumnDataEncoder: Encoder[Float32ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putFloat)
  }

  val float64ColumnDataEncoder: Encoder[Float64ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putDouble)
  }

  val int8ColumnDataEncoder: Encoder[Int8ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.put)
  }

  val int16ColumnDataEncoder: Encoder[Int16ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putShort)
  }

  val int32ColumnDataEncoder: Encoder[Int32ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putInt)
  }

  val int64ColumnDataEncoder: Encoder[Int64ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putLong)
  }

  def nullableColumnDataEncoder(encoder: Encoder[ColumnData]): Encoder[NullableColumnData] = Encoder { (col, buf) =>
    col.nulls.foreach { b =>
      val byte: Byte = if(b) 1 else 0
      buf.put(byte)
    }

    encoder.write(col.data, buf)
  }

  val stringColumnDataEncoder: Encoder[StringColumnData] = Encoder { (col, buf) =>
    col.data.foreach(writeString(_, buf))
  }

  val uint8ColumnDataEncoder: Encoder[UInt8ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.put)
  }

  val uint16ColumnDataEncoder: Encoder[UInt16ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putShort)
  }

  val uint32ColumnDataEncoder: Encoder[UInt32ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putInt)
  }

  val uint64ColumnDataEncoder: Encoder[UInt64ColumnData] = Encoder { (col, buf) =>
    col.data.foreach(buf.putLong)
  }

  val uuidColumnDataEncoder: Encoder[UuidColumnData] = Encoder { (col, buf) =>
    col.data.foreach { uuid =>
      buf.putLong(uuid.getMostSignificantBits())
      buf.putLong(uuid.getLeastSignificantBits())
    }
  }

  val columnDataEncoder: Encoder[ColumnData] = Encoder { (col, buf) =>
    val colType: String = ColumnDataUtil.getType(col)
    writeString(colType, buf)

    columnDataOnlyEncoder.write(col, buf)
  }

  val columnDataOnlyEncoder: Encoder[ColumnData] = Encoder { (col, buf) =>
    col match {
      case col: DateColumnData => dateColumnDataEncoder.write(col, buf)
      case col: DateTimeColumnData => dateTimeColumnDataEncoder.write(col, buf)
      case col: Enum8ColumnData => enum8ColumnDataEncoder.write(col, buf)
      case col: Enum16ColumnData => enum16ColumnDataEncoder.write(col, buf)
      case col: FixedStringColumnData => fixedStringColumnDataEncoder.write(col, buf)
      case col: Float32ColumnData => float32ColumnDataEncoder.write(col, buf)
      case col: Float64ColumnData => float64ColumnDataEncoder.write(col, buf)
      case col: Int8ColumnData => int8ColumnDataEncoder.write(col, buf)
      case col: Int16ColumnData => int16ColumnDataEncoder.write(col, buf)
      case col: Int32ColumnData => int32ColumnDataEncoder.write(col, buf)
      case col: Int64ColumnData => int64ColumnDataEncoder.write(col, buf)
      case col: NullableColumnData => nullableColumnDataEncoder(columnDataOnlyEncoder).write(col, buf)
      case col: StringColumnData => stringColumnDataEncoder.write(col, buf)
      case col: UInt8ColumnData => uint8ColumnDataEncoder.write(col, buf)
      case col: UInt16ColumnData => uint16ColumnDataEncoder.write(col, buf)
      case col: UInt32ColumnData => uint32ColumnDataEncoder.write(col, buf)
      case col: UInt64ColumnData => uint64ColumnDataEncoder.write(col, buf)
      case col: UuidColumnData => uuidColumnDataEncoder.write(col, buf)
    } 
  }
}

object ColumnDataUtil {
  def getType(col: ColumnData): String = col match {
    case _: DateColumnData => "Date"
    case _: DateTimeColumnData => "DateTime"
    case col: Enum8ColumnData =>
      val enumDefs: String = col.enums.toList.sortBy(_._1).map(t => s"'${t._2}' = ${t._1}").mkString(",")
      s"Enum8($enumDefs)"
    case col: Enum16ColumnData =>
      val enumDefs: String = col.enums.toList.sortBy(_._1).map(t => s"'${t._2}' = ${t._1}").mkString(",")
      s"Enum16($enumDefs)"
    case _: FixedStringColumnData => "FixedString"
    case _: Float32ColumnData => "Float32"
    case _: Float64ColumnData => "Float64"
    case _: Int8ColumnData => "Int8"
    case _: Int16ColumnData => "Int16"
    case _: Int32ColumnData => "Int32"
    case _: Int64ColumnData => "Int64"
    case col: NullableColumnData => s"Nullable(${getType(col.data)})"
    case _: StringColumnData => "String"
    case _: UInt8ColumnData => "UInt8"
    case _: UInt16ColumnData => "UInt16"
    case _: UInt32ColumnData => "UInt32"
    case _: UInt64ColumnData => "UInt64"
    case _: UuidColumnData => "UUID"
  }
}