package scalackh.transpose

import java.time.LocalDate

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import scalackh.protocol._

trait DefaultColumnTransposers {
  implicit val columnTransposerString: ColumnTransposer[String] = ColumnTransposerInstances.columnTransposerBuilder(
    StringColumnData(_),
    { case StringColumnData(data) => data }
  )

  implicit val columnTransposerInt: ColumnTransposer[Int] = ColumnTransposerInstances.columnTransposerBuilder(
    Int32ColumnData(_),
    { case Int32ColumnData(data) => data }
  )

  implicit val columnTransposerDouble: ColumnTransposer[Double] = ColumnTransposerInstances.columnTransposerBuilder(
    Float64ColumnData(_),
    { case Float64ColumnData(data) => data }
  )

  implicit val columnTransposerDate: ColumnTransposer[LocalDate] = ColumnTransposerInstances.columnTransposerBuilder(
    DateColumnData(_),
    { case DateColumnData(data) => data }
  )
}

object ColumnTransposerInstances {
  def columnTransposerBuilder[T](
    colBuilder: Array[T] => ColumnData,
    colPartial: PartialFunction[ColumnData, Array[T]]
  )(implicit ct: ClassTag[T]): ColumnTransposer[T] = new ColumnTransposer[T] {

    def toColumns(l: List[T]): List[ColumnData] = List(colBuilder(l.toArray))

    def fromColumns(l: List[ColumnData]): Try[List[T]] = {
      if(l.length != 1) Failure(new RuntimeException(s"Not exactly one column: ${l.length}"))
      else {
        if(colPartial.isDefinedAt(l.head)) Success(colPartial.apply(l.head).toList)
        else Failure(new RuntimeException(s"ColumnTransposer for ${ct} but got ${l.head}"))
      }
    }
  }
}