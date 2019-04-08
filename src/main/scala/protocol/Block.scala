package scalackh.protocol

case class Block(
  table: Option[String],
  info: BlockInfo,
  nbColumns: Int,
  nbRows: Int,
  columns: List[Column[ColumnData]]
) {
  // fail here with a meaningfull description instead of the internals of protocol
  assert(nbColumns == columns.length, s"nbColumns does not match the count of columns ($nbColumns != ${columns.length})")
}

object Block {
  val empty: Block = Block(None, BlockInfo.empty, 0, 0, List.empty)
}

case class BlockInfo(
  isOverflow: Boolean,
  bucketNum: Int
)

object BlockInfo {
  val empty: BlockInfo = BlockInfo(false, -1)
}