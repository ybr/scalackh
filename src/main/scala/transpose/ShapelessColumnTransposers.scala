package scalackh.transpose

import scala.util.{Failure, Success, Try}

import shapeless._

import scalackh.protocol._

trait ShapelessColumnTransposers {
  implicit val columnTransposerHNil: ColumnTransposer[HNil] = new ColumnTransposer[HNil] {
    def toColumnsData(p: List[HNil]): List[ColumnData] = List.empty

    def fromColumnsData(l: List[ColumnData]): Try[List[HNil]] = Success(List.empty)
  }

  implicit def columnizeCons[H, T <: HList](
    implicit
      hc: Lazy[ColumnTransposer[H]],
      tc: Lazy[ColumnTransposer[T]]
  ): ColumnTransposer[H :: T] = new ColumnTransposer[H :: T] {
    def toColumnsData(p: List[H :: T]): List[ColumnData] = {
      val heads = p.map(_.head)
      val tails = p.map(_.tail)
      hc.value.toColumnsData(heads) ++ tc.value.toColumnsData(tails)
    }

    def fromColumnsData(cols: List[ColumnData]): Try[List[H :: T]] = {
      cols.headOption.fold[Try[List[H :: T]]](Failure(new RuntimeException("Not enough columns to feed shapeless column transposer"))) { col =>
        for {
          heads <- hc.value.fromColumnsData(List(col))
          tails <- tc.value.fromColumnsData(cols.tail)
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
    def toColumnsData(f: List[F]): List[ColumnData] = cg.toColumnsData(f.map(gen.to))

    def fromColumnsData(cols: List[ColumnData]): Try[List[F]] = cg.fromColumnsData(cols).map(_.map(gen.from))
  }
}