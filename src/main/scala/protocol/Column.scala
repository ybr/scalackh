package scalackh.protocol

// import java.math.BigInteger
import java.time.{LocalDate, LocalDateTime}
// import java.util.UUID

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
// case class UInt8ColumnData(data: Array[Short]) extends IntegerColumnData
// case class UInt16ColumnData(data: Array[Int]) extends IntegerColumnData
// case class UInt32ColumnData(data: Array[Long]) extends IntegerColumnData
// case class UInt64ColumnData(data: Array[BigInteger]) extends IntegerColumnData

// sealed trait EnumColumnData extends ColumnData
// case class Enum8ColumnData(enums: Map[Int, String], data: Array[Int]) extends EnumColumnData
// case class Enum16ColumnData(enums: Map[Int, String], data: Array[Int]) extends EnumColumnData

// case class ArrayColumnData(data: ClickhouseArray) extends ColumnData
// case class NullableColumnData(nulls: Array[Boolean], data: ColumnData) extends ColumnData
case class StringColumnData(data: Array[String]) extends ColumnData
// case class FixedStringColumnData(strLength: Int, data: Array[String]) extends ColumnData
// case class UuidColumnData(data: Array[UUID]) extends ColumnData
// case class TupleColumnData(data: Array[TupleData]) extends ColumnData

// sealed trait ClickhouseArray
// sealed trait ScalaNativeArray extends ClickhouseArray
// case class DateArray(dates: Array[Array[LocalDate]]) extends ScalaNativeArray
// case class DateTimeArray(datetimes: Array[Array[LocalDateTime]]) extends ScalaNativeArray
// case class Float32Array(floats: Array[Array[Float]]) extends ScalaNativeArray
// case class Float64Array(doubles: Array[Array[Double]]) extends ScalaNativeArray
// case class Int8Array(bytes: Array[Array[Byte]]) extends ScalaNativeArray
// case class Int16Array(shorts: Array[Array[Short]]) extends ScalaNativeArray
// case class Int32Array(ints: Array[Array[Int]]) extends ScalaNativeArray
// case class Int64Array(longs: Array[Array[Long]]) extends ScalaNativeArray
// case class StringArray(strings: Array[Array[String]]) extends ScalaNativeArray

// sealed trait CellData
// case class DateData(data: LocalDate) extends CellData
// case class DateTimeData(data: LocalDateTime) extends CellData
// // case class Enum8Data(data: Byte) extends CellData
// // case class Enum16Data(data: Short) extends CellData
// case class Float32Data(data: Float) extends CellData
// case class Float64Data(data: Double) extends CellData
// case class Int8Data(data: Byte) extends CellData
// case class Int16Data(data: Short) extends CellData
// case class Int32Data(data: Int) extends CellData
// case class Int64Data(data: Long) extends CellData
// case class StringData(data: String) extends CellData
// // case class TupleData(elements: List[CellData]) extends CellData
// // case class UInt8Data(data: Short) extends CellData
// // case class UInt16Data(data: Int) extends CellData
// // case class UInt32Data(data: Long) extends CellData
// // case class UInt64Data(data: BigInteger) extends CellData
// // case class UuidData(data: UUID) extends CellData