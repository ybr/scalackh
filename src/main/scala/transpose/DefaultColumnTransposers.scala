package scalackh.transpose

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import scalackh.protocol._

trait DefaultColumnTransposers {
  implicit val columnTransposerString: ColumnTransposer[String] = ColumnTransposerInstances.columnTransposerBuilder(
    StringColumnData(_),
    {
      case StringColumnData(data) => data
      case FixedStringColumnData(_, data) => data
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