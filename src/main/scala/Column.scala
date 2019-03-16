package ckh.native

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

sealed trait Column
case class ArrayColumn(name: String, data: ClickhouseArray) extends Column
case class DateColumn(name: String, data: Array[LocalDate]) extends Column
case class DateTimeColumn(name: String, data: Array[LocalDateTime]) extends Column // UTC zone offset
case class EnumColumn(name: String, bits: Int, enums: Map[Int, String], data: Array[Int]) extends Column
case class FixedStringColumn(name: String, strLength: Int, data: Array[String]) extends Column
case class Float32Column(name: String, data: Array[Float]) extends Column
case class Float64Column(name: String, data: Array[Double]) extends Column
case class Int8Column(name: String, data: Array[Byte]) extends Column
case class Int16Column(name: String, data: Array[Short]) extends Column
case class Int32Column(name: String, data: Array[Int]) extends Column
case class Int64Column(name: String, data: Array[Long]) extends Column
case class NullableColumn(name: String, nulls: Array[Boolean], column: Column) extends Column
case class StringColumn(name: String, data: Array[String]) extends Column
case class UuidColumn(name: String, data: Array[UUID]) extends Column
case class TupleColumn(name: String, data: Array[TupleData]) extends Column

sealed trait ClickhouseArray
sealed trait ScalaNativeArray extends ClickhouseArray
case class DateArray(dates: Array[Array[LocalDate]]) extends ScalaNativeArray
case class DateTimeArray(datetimes: Array[Array[LocalDateTime]]) extends ScalaNativeArray
case class DoubleArray(doubles: Array[Array[Double]]) extends ScalaNativeArray
case class FloatArray(floats: Array[Array[Float]]) extends ScalaNativeArray
case class ByteArray(bytes: Array[Array[Byte]]) extends ScalaNativeArray
case class ShortArray(shorts: Array[Array[Short]]) extends ScalaNativeArray
case class IntArray(ints: Array[Array[Int]]) extends ScalaNativeArray
case class LongArray(longs: Array[Array[Long]]) extends ScalaNativeArray
case class StringArray(strings: Array[Array[String]]) extends ScalaNativeArray


case class ClickhouseGenericArray(array: Array[Array[ClickhouseData]]) extends ClickhouseArray

sealed trait ClickhouseData
case class ByteData(data: Byte) extends ClickhouseData
case class DateData(data: LocalDate) extends ClickhouseData
case class DateTimeData(data: LocalDateTime) extends ClickhouseData
case class Enum8Data(data: Byte) extends ClickhouseData
case class Enum16Data(data: Short) extends ClickhouseData
case class Float32Data(data: Float) extends ClickhouseData
case class Float64Data(data: Double) extends ClickhouseData
case class Int8Data(data: Byte) extends ClickhouseData
case class Int16Data(data: Short) extends ClickhouseData
case class Int32Data(data: Int) extends ClickhouseData
case class Int64Data(data: Long) extends ClickhouseData
case class StringData(data: String) extends ClickhouseData
case class TupleData(elements: List[ClickhouseData]) extends ClickhouseData
case class UuidData(data: UUID) extends ClickhouseData