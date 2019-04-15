package scalackh.protocol

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

case class Column[A](name: String, data: A)

sealed trait ColumnData

sealed trait TemporalColumnData extends ColumnData
case class DateColumnData(data: Array[LocalDate]) extends TemporalColumnData
case class DateTimeColumnData(data: Array[LocalDateTime]) extends TemporalColumnData // UTC zone offset

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