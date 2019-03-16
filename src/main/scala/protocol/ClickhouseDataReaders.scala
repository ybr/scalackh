package ckh.protocol

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import ckh.native._
import DefaultReaders._

object ClickhouseDataReaders {
  val tuple = "Tuple\\((.+)\\)".r

  def dataReader(dataType: String): Reader[ClickhouseData] = Reader { buf =>
    dataType match {
      case "Date" => dateReader.read(buf)
      case "DateTime" => datetimeReader.read(buf)
      case "Float32" => float32Reader.read(buf)
      case "Float64" => float64Reader.read(buf)
      case "Int8" => int8Reader.read(buf)
      case "Int16" => int16Reader.read(buf)
      case "Int32" => int32Reader.read(buf)
      case "Int64" => int64Reader.read(buf)
      case "String" => stringReader.read(buf)
      case tuple(types) => tupleReader(types).read(buf)
      case "UUID" => uuidReader.read(buf)
      case other => throw new UnsupportedOperationException(s"Unsupported data type ${other}")
    }
  }

  val dateReader: Reader[DateData] = Reader { buf =>
    DateData(LocalDate.ofEpochDay(readShort(buf)))
  }

  val datetimeReader: Reader[DateTimeData] = Reader { buf =>
    DateTimeData(LocalDateTime.ofEpochSecond(readInt(buf), 0, ZoneOffset.UTC))
  }

  val enum8Reader: Reader[Enum8Data] = Reader(buf => Enum8Data(buf.get))
  val enum16Reader: Reader[Enum16Data] = Reader(buf => Enum16Data(readShort(buf)))

  val float32Reader: Reader[Float32Data] = Reader(buf => Float32Data(readFloat(buf)))

  val float64Reader: Reader[Float64Data] = Reader(buf => Float64Data(readDouble(buf)))

  val int8Reader: Reader[Int8Data] = Reader(buf => Int8Data(buf.get()))

  val int16Reader: Reader[Int16Data] = Reader(buf => Int16Data(readShort(buf)))

  val int32Reader: Reader[Int32Data] = Reader(buf => Int32Data(readInt(buf)))

  val int64Reader: Reader[Int64Data] = Reader(buf => Int64Data(readLong(buf)))

  val stringReader: Reader[StringData] = Reader(buf => StringData(readString(buf)))
      
  def tupleReader(typesStr: String): Reader[TupleData] = {
    val types = typesStr.split(",").map(_.trim)

    val readers: List[Reader[ClickhouseData]] = types.map(dataReader).toList

    Reader(buf => TupleData(readers.map(_.read(buf))))
  }

  val uuidReader: Reader[UuidData] = Reader { buf =>
    val mostSigBits: Long = readLong(buf)
    val leastSigBits: Long = readLong(buf)
    UuidData(new UUID(mostSigBits, leastSigBits))
  }
}