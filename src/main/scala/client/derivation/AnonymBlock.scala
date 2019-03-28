package scalackh.client.derivation

import scalackh.protocol._

import scala.language.postfixOps

case class AnonymBlock(
  nbColumns: Int,
  nbRows: Int,
  columnsData: List[ColumnData]
) {
  def name(table: Option[String] = None, columnsName: List[String]): Block = {
    Block(
      table,
      BlockInfo.empty,
      nbColumns,
      nbRows,
      columnsName.zip(columnsData).map(Column.apply[ColumnData] _ tupled)
    )
  }
}