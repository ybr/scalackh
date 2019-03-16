package ckh.protocol

import java.math.BigInteger
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
      case "UInt8" => uint8Reader.read(buf)
      case "UInt16" => uint16Reader.read(buf)
      case "UInt32" => uint32Reader.read(buf)
      case "UInt64" => uint64Reader.read(buf)
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

  val uint8Reader: Reader[UInt8Data] = Reader { buf =>
    UInt8Data((0xff & buf.get()).toShort)
  }

  val uint16Reader: Reader[UInt16Data] = Reader { buf =>
    UInt16Data((0xffff & readShort(buf)).toInt)
  }

  val uint32Reader: Reader[UInt32Data] = {
    val bytes: Array[Byte] = new Array(4)
    Reader { buf =>
      buf.get(bytes)

      var result: Long = bytes(3) & 0xff
      result = (result << 8) + (bytes(2) & 0xff)
      result = (result << 8) + (bytes(1) & 0xff)
      result = (result << 8) + (bytes(0) & 0xff)

      UInt32Data(result)
    }
  }

  val uint64Reader: Reader[UInt64Data] = {
    val bytes: Array[Byte] = new Array(8)
    Reader { buf =>
      bytes(7) = buf.get()
      bytes(6) = buf.get()
      bytes(5) = buf.get()
      bytes(4) = buf.get()
      bytes(3) = buf.get()
      bytes(2) = buf.get()
      bytes(1) = buf.get()
      bytes(0) = buf.get()

      UInt64Data(new BigInteger(bytes))
    }
  }

  val uuidReader: Reader[UuidData] = Reader { buf =>
    val mostSigBits: Long = readLong(buf)
    val leastSigBits: Long = readLong(buf)
    UuidData(new UUID(mostSigBits, leastSigBits))
  }
}