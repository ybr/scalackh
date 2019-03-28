package scalackh.transpose

import scala.util.Try

import scalackh.protocol._

trait ColumnTransposer[T] {
  def toColumnsData(t: List[T]): List[ColumnData]

  def fromColumnsData(l: List[ColumnData]): Try[List[T]]
}