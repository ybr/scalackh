package scalackh.protocol

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import scalackh.math.{UInt16, UInt32}

case class Column[A](name: String, data: A)

sealed trait ColumnData

case class DateColumnData(data: Array[Short]) extends ColumnData
case class DateTimeColumnData(data: Array[Int]) extends ColumnData // UTC zone offset

case class Float32ColumnData(data: Array[Float]) extends ColumnData
case class Float64ColumnData(data: Array[Double]) extends ColumnData

case class Int8ColumnData(data: Array[Byte]) extends ColumnData
case class Int16ColumnData(data: Array[Short]) extends ColumnData
case class Int32ColumnData(data: Array[Int]) extends ColumnData
case class Int64ColumnData(data: Array[Long]) extends ColumnData
case class UInt8ColumnData(data: Array[Byte]) extends ColumnData
case class UInt16ColumnData(data: Array[Short]) extends ColumnData
case class UInt32ColumnData(data: Array[Int]) extends ColumnData
case class UInt64ColumnData(data: Array[Long]) extends ColumnData

case class Enum8ColumnData(enums: Map[Int, String], data: Array[Byte]) extends ColumnData
case class Enum16ColumnData(enums: Map[Int, String], data: Array[Short]) extends ColumnData

case class ArrayColumnData(data: ClickhouseArray) extends ColumnData
case class NullableColumnData(nulls: Array[Boolean], data: ColumnData) extends ColumnData
case class StringColumnData(data: Array[String]) extends ColumnData
case class FixedStringColumnData(strLength: Int, data: Array[String]) extends ColumnData
case class UuidColumnData(data: Array[UUID]) extends ColumnData

object ColumnData {
  def nbRows(colData: ColumnData): Int = colData match {
    case DateColumnData(data) => data.length
    case DateTimeColumnData(data) => data.length
    case Float32ColumnData(data) => data.length
    case Float64ColumnData(data) => data.length
    case Int8ColumnData(data) => data.length
    case Int16ColumnData(data) => data.length
    case Int32ColumnData(data) => data.length
    case Int64ColumnData(data) => data.length
    case UInt8ColumnData(data) => data.length
    case UInt16ColumnData(data) => data.length
    case UInt32ColumnData(data) => data.length
    case UInt64ColumnData(data) => data.length
    case Enum8ColumnData(_, data) => data.length
    case Enum16ColumnData(_, data) => data.length
    case ArrayColumnData(array) => array match {
      case DateArray(data) => data.length
      case DateTimeArray(data) => data.length
      case Float32Array(data) => data.length
      case Float64Array(data) => data.length
      case Int8Array(data) => data.length
      case Int16Array(data) => data.length
      case Int32Array(data) => data.length
      case Int64Array(data) => data.length
      case StringArray(data) => data.length
      case UInt8Array(data) => data.length
      case UInt16Array(data) => data.length
      case UInt32Array(data) => data.length
      case UInt64Array(data) => data.length
      case UuidArray(data) => data.length
    } 
    case NullableColumnData(_, data) => nbRows(data)
    case StringColumnData(data) => data.length
    case FixedStringColumnData(_, data) => data.length
    case UuidColumnData(data) => data.length
  }
}

object DateColumnData {
  def fromLocalDate(dates: Array[LocalDate]): Array[Short] = dates.map(_.toEpochDay().toShort)

  def toLocalDate(shorts: Array[Short]): Array[LocalDate] = shorts.map { short =>
    LocalDate.ofEpochDay(UInt16.unsign(short).toLong)
  }
}

object DateTimeColumnData {
  def fromLocalDateTime(times: Array[LocalDateTime], zone: ZoneOffset): Array[Int] = times.map(_.toEpochSecond(zone).toInt)

  def fromLocalDateTime(times: Array[LocalDateTime]): Array[Int] = fromLocalDateTime(times, ZoneOffset.UTC)

  def toLocalDateTime(ints: Array[Int], zone: ZoneOffset): Array[LocalDateTime] = ints.map { int =>
    LocalDateTime.ofEpochSecond(UInt32.unsign(int), 0, zone)
  }

  def toLocalDateTime(ints: Array[Int]): Array[LocalDateTime] = toLocalDateTime(ints, ZoneOffset.UTC)
}