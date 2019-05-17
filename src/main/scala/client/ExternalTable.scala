package scalackh

import scalackh.protocol.{Block, BlockInfo, Column, ColumnData}

case class ExternalTableBuilder(table: String, columnNames: List[String]) {
  def build(columnData: List[ColumnData]): Block = BlockBuilder.build(Some(table), columnNames, columnData)
}

object BlockBuilder {
  def build(table: Option[String], columnNames: List[String], columnData: List[ColumnData]): Block = {
    // fail here with a meaningfull description instead of the internals of protocol
    val nbNames = columnNames.length
    val nbData = columnData.length
    assert(nbNames == nbData, s"Count of names does not match count of data columns ($nbNames != $nbData)")

    val nbRows = columnData.headOption.map(ColumnData.nbRows).getOrElse(0)
    assert(columnData.forall(data => ColumnData.nbRows(data) == nbRows), "All columns does not have the same count of rows")

    Block(
      table = table,
      info = BlockInfo.empty,
      nbColumns = columnNames.length,
      // supposes all data columns have the same number of rows
      nbRows = columnData.headOption.map(ColumnData.nbRows).getOrElse(0),
      columns = columnNames.zip(columnData).map { case (name, data) =>
        Column(name, data)
      }
    )
  }
}