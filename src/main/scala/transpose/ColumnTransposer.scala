package scalackh.transpose

import scala.util.Try

import scalackh.protocol._

trait ColumnTransposer[T] {
  def toColumns(t: List[T]): List[ColumnData]

  def fromColumns(l: List[ColumnData]): Try[List[T]]
}