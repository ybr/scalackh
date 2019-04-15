package scalackh.protocol

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import scalackh.math.{UInt16, UInt32}

case class Column[A](name: String, data: A)

sealed trait ColumnData

sealed trait TemporalColumnData extends ColumnData
case class DateColumnData(data: Array[Short]) extends TemporalColumnData
case class DateTimeColumnData(data: Array[Int]) extends TemporalColumnData // UTC zone offset

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

sealed trait FloatingColumnData extends ColumnData
case class Float32ColumnData(data: Array[Float]) extends FloatingColumnData
case class Float64ColumnData(data: Array[Double]) extends FloatingColumnData

sealed trait IntegerColumnData extends ColumnData
case class Int8ColumnData(data: Array[Byte]) extends IntegerColumnData
case class Int16ColumnData(data: Array[Short]) extends IntegerColumnData
case class Int32ColumnData(data: Array[Int]) extends IntegerColumnData
case class Int64ColumnData(data: Array[Long]) extends IntegerColumnData
case class UInt8ColumnData(data: Array[Byte]) extends IntegerColumnData
case class UInt16ColumnData(data: Array[Short]) extends IntegerColumnData
case class UInt32ColumnData(data: Array[Int]) extends IntegerColumnData
case class UInt64ColumnData(data: Array[Long]) extends IntegerColumnData

sealed trait EnumColumnData extends ColumnData
case class Enum8ColumnData(enums: Map[Int, String], data: Array[Byte]) extends EnumColumnData
case class Enum16ColumnData(enums: Map[Int, String], data: Array[Short]) extends EnumColumnData

case class ArrayColumnData(data: ClickhouseArray) extends ColumnData
case class NullableColumnData(nulls: Array[Boolean], data: ColumnData) extends ColumnData
case class StringColumnData(data: Array[String]) extends ColumnData
case class FixedStringColumnData(strLength: Int, data: Array[String]) extends ColumnData
case class UuidColumnData(data: Array[UUID]) extends ColumnData