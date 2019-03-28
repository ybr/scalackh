package scalackh.client.derivation

import scalackh.protocol._
import scalackh.protocol.steps.ProtocolSteps
import scalackh.transpose.ColumnTransposer

object DerivationUtils {
  def blockIterator[A](as: Iterator[A])(implicit colA: ColumnTransposer[A]): Iterator[Block] = as.grouped(ProtocolSteps.MAX_ROWS_IN_BLOCK).map { group =>
    val valuesForBlock = group.toList
    val columns: List[ColumnData] = colA.toColumnsData(valuesForBlock)
    Block(None, BlockInfo.empty, columns.length, valuesForBlock.length, columns.map(Column("", _)))
  }

  def anonymeBlocks[A](as: Iterator[A])(implicit colA: ColumnTransposer[A]): Iterator[AnonymBlock] = as.grouped(ProtocolSteps.MAX_ROWS_IN_BLOCK).map { group =>
    val valuesForBlock = group.toList
    val columnsData: List[ColumnData] = colA.toColumnsData(valuesForBlock)
    AnonymBlock(columnsData.length, valuesForBlock.length, columnsData)
  }
}