package scalackh.transpose

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import scalackh.math._
import scalackh.protocol._

trait DefaultColumnTransposers {
  implicit def columnTransposerNullable[A](implicit ctA: ColumnTransposer[A], nullable: Nullable[A], ct: ClassTag[A]): ColumnTransposer[Option[A]] = {
    val nullA: A = nullable.nullValue
    ColumnTransposerInstances.columnTransposerBuilder[Option[A]](
      maybeAs => {
        val (nulls, as) = maybeAs.map {
          case Some(a) => (false, a)
          case None => (true, nullA)
        }.unzip
        NullableColumnData(nulls.toArray, ctA.toColumnsData(as.toList).head)
      },
      {
        case NullableColumnData(nulls, data) =>
          val as = ctA.fromColumnsData(List(data)).get
          as.zipWithIndex.map { case (a, i) =>
            val nullableA: Option[A] = if(nulls(i)) None else Some(a)
            nullableA
          }.toArray
      }
    )
  }

  implicit val columnTransposerString: ColumnTransposer[String] = ColumnTransposerInstances.columnTransposerBuilder(
    StringColumnData(_),
    {
      case StringColumnData(data) => data
    }
  )

  implicit val columnTransposerByte: ColumnTransposer[Byte] = ColumnTransposerInstances.columnTransposerBuilder(
    Int8ColumnData(_),
    { case Int8ColumnData(data) => data }
  )

  implicit val columnTransposerShort: ColumnTransposer[Short] = ColumnTransposerInstances.columnTransposerBuilder(
    Int16ColumnData(_),
    { case Int16ColumnData(data) => data }
  )

  implicit val columnTransposerInt: ColumnTransposer[Int] = ColumnTransposerInstances.columnTransposerBuilder(
    Int32ColumnData(_),
    { case Int32ColumnData(data) => data }
  )

  implicit val columnTransposerLong: ColumnTransposer[Long] = ColumnTransposerInstances.columnTransposerBuilder(
    Int64ColumnData(_),
    { case Int64ColumnData(data) => data }
  )

  implicit val columnTransposerFloat: ColumnTransposer[Float] = ColumnTransposerInstances.columnTransposerBuilder(
    Float32ColumnData(_),
    { case Float32ColumnData(data) => data }
  )

  implicit val columnTransposerDouble: ColumnTransposer[Double] = ColumnTransposerInstances.columnTransposerBuilder(
    Float64ColumnData(_),
    { case Float64ColumnData(data) => data }
  )

  implicit val columnTransposerDate: ColumnTransposer[LocalDate] = ColumnTransposerInstances.columnTransposerBuilder(
    DateColumnData(_),
    { case DateColumnData(data) => data }
  )

  implicit val columnTransposerDateTime: ColumnTransposer[LocalDateTime] = ColumnTransposerInstances.columnTransposerBuilder(
    DateTimeColumnData(_),
    { case DateTimeColumnData(data) => data }
  )

  implicit val columnTransposerUInt8: ColumnTransposer[UInt8] = ColumnTransposerInstances.columnTransposerBuilder(
    ui8s => UInt8ColumnData(ui8s.map(_.unsafeByte)),
    { case UInt8ColumnData(data) => data.map(UInt8.unsign) }
  )

  implicit val columnTransposerUInt16: ColumnTransposer[UInt16] = ColumnTransposerInstances.columnTransposerBuilder(
    ui16s => UInt16ColumnData(ui16s.map(_.unsafeShort)),
    { case UInt16ColumnData(data) => data.map(UInt16.unsign) }
  )

  implicit val columnTransposerUInt32: ColumnTransposer[UInt32] = ColumnTransposerInstances.columnTransposerBuilder(
    ui32s => UInt32ColumnData(ui32s.map(_.unsafeInt)),
    { case UInt32ColumnData(data) => data.map(UInt32.unsign) }
  )

  implicit val columnTransposerUInt64: ColumnTransposer[UInt64] = ColumnTransposerInstances.columnTransposerBuilder(
    ui64s => UInt64ColumnData(ui64s.map(_.unsafeLong)),
    { case UInt64ColumnData(data) => data.map(UInt64.unsign) }
  )

  implicit val uuidTransposerString: ColumnTransposer[UUID] = ColumnTransposerInstances.columnTransposerBuilder(
    UuidColumnData(_),
    { case UuidColumnData(data) => data }
  )
}

object ColumnTransposerInstances {
  def columnTransposerBuilder[T](
    colBuilder: Array[T] => ColumnData,
    colPartial: PartialFunction[ColumnData, Array[T]]
  )(implicit ct: ClassTag[T]): ColumnTransposer[T] = new ColumnTransposer[T] {

    def toColumnsData(l: List[T]): List[ColumnData] = List(colBuilder(l.toArray))

    def fromColumnsData(l: List[ColumnData]): Try[List[T]] = {
      if(l.length != 1) Failure(new RuntimeException(s"Not exactly one column got ${l.length}"))
      else {
        if(colPartial.isDefinedAt(l.head)) Success(colPartial.apply(l.head).toList)
        else Failure(new RuntimeException(s"ColumnTransposer for ${ct} but got ${l.head}"))
      }
    }
  }
}