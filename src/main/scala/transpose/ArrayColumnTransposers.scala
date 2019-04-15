package scalackh.transpose

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import scalackh.math._
import scalackh.protocol._

trait ArrayColumnTransposers {
  implicit val columnTransposerArrayLocalDate: ColumnTransposer[Array[LocalDate]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(DateArray(array)),
    { case ArrayColumnData(DateArray(data)) => data }
  )

  implicit val columnTransposerArrayLocalDateTime: ColumnTransposer[Array[LocalDateTime]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(DateTimeArray(array)),
    { case ArrayColumnData(DateTimeArray(data)) => data }
  )

  implicit val columnTransposerArrayFloat: ColumnTransposer[Array[Float]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Float32Array(array)),
    { case ArrayColumnData(Float32Array(data)) => data }
  )

  implicit val columnTransposerArrayDouble: ColumnTransposer[Array[Double]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Float64Array(array)),
    { case ArrayColumnData(Float64Array(data)) => data }
  )

  implicit val columnTransposerArrayByte: ColumnTransposer[Array[Byte]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Int8Array(array)),
    { case ArrayColumnData(Int8Array(data)) => data }
  )

  implicit val columnTransposerArrayShort: ColumnTransposer[Array[Short]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Int16Array(array)),
    { case ArrayColumnData(Int16Array(data)) => data }
  )

  implicit val columnTransposerArrayInt: ColumnTransposer[Array[Int]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Int32Array(array)),
    { case ArrayColumnData(Int32Array(data)) => data }
  )

  implicit val columnTransposerArrayLong: ColumnTransposer[Array[Long]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(Int64Array(array)),
    { case ArrayColumnData(Int64Array(data)) => data }
  )

  implicit val columnTransposerArrayString: ColumnTransposer[Array[String]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(StringArray(array)),
    { case ArrayColumnData(StringArray(data)) => data }
  )

  implicit val columnTransposerArrayUInt8: ColumnTransposer[Array[UInt8]] = ColumnTransposerInstances.columnTransposerBuilder(
    ui8s => ArrayColumnData(UInt8Array(ui8s.map(_.map(_.unsafeByte)))),
    { case ArrayColumnData(UInt8Array(data)) => data.map(_.map(UInt8.unsign)) }
  )

  implicit val columnTransposerArrayUInt16: ColumnTransposer[Array[UInt16]] = ColumnTransposerInstances.columnTransposerBuilder(
    ui16s => ArrayColumnData(UInt16Array(ui16s.map(_.map(_.unsafeShort)))),
    { case ArrayColumnData(UInt16Array(data)) => data.map(_.map(UInt16.unsign)) }
  )

  implicit val columnTransposerArrayUInt32: ColumnTransposer[Array[UInt32]] = ColumnTransposerInstances.columnTransposerBuilder(
    ui32s => ArrayColumnData(UInt32Array(ui32s.map(_.map(_.unsafeInt)))),
    { case ArrayColumnData(UInt32Array(data)) => data.map(_.map(UInt32.unsign)) }
  )

  implicit val columnTransposerArrayUInt64: ColumnTransposer[Array[UInt64]] = ColumnTransposerInstances.columnTransposerBuilder(
    ui64s => ArrayColumnData(UInt64Array(ui64s.map(_.map(_.unsafeLong)))),
    { case ArrayColumnData(UInt64Array(data)) => data.map(_.map(UInt64.unsign)) }
  )

  implicit val columnTransposerArrayUuid: ColumnTransposer[Array[UUID]] = ColumnTransposerInstances.columnTransposerBuilder(
    array => ArrayColumnData(UuidArray(array)),
    { case ArrayColumnData(UuidArray(data)) => data }
  )
}