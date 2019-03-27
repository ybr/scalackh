package scalackh.transpose

import scala.util.{Failure, Success, Try}

import shapeless._

import scalackh.protocol._

trait ShapelessColumnTransposers {
  implicit val columnTransposerHNil: ColumnTransposer[HNil] = new ColumnTransposer[HNil] {
    def toColumns(p: List[HNil]): List[ColumnData] = List.empty

    def fromColumns(l: List[ColumnData]): Try[List[HNil]] = Success(List.empty)
  }

  implicit def columnizeCons[H, T <: HList](
    implicit
      hc: Lazy[ColumnTransposer[H]],
      tc: Lazy[ColumnTransposer[T]]
  ): ColumnTransposer[H :: T] = new ColumnTransposer[H :: T] {
    def toColumns(p: List[H :: T]): List[ColumnData] = {
      val heads = p.map(_.head)
      val tails = p.map(_.tail)
      hc.value.toColumns(heads) ++ tc.value.toColumns(tails)
    }

    def fromColumns(cols: List[ColumnData]): Try[List[H :: T]] = {
      cols.headOption.fold[Try[List[H :: T]]](Failure(new RuntimeException("Not enough columns to feed columnTransposer"))) { col =>
        for {
          heads <- hc.value.fromColumns(List(col))
          tails <- tc.value.fromColumns(cols.tail)
        } yield {
          if(tails.isEmpty) { // if tails is empty we certainly reached HNil
            val emptyT = HNil.asInstanceOf[T]
            heads.map(_ :: emptyT)
          }
          else heads.zip(tails).map { case (h, t) => h :: t } // safe to zip with tails since it is not empty
        }
      }
    }
  }

  implicit def columnzeDeriveInstance[F, G](implicit gen: Generic.Aux[F, G], cg: ColumnTransposer[G]): ColumnTransposer[F] = new ColumnTransposer[F] {
    def toColumns(f: List[F]): List[ColumnData] = cg.toColumns(f.map(gen.to))

    def fromColumns(cols: List[ColumnData]): Try[List[F]] = cg.fromColumns(cols).map(_.map(gen.from))
  }
}