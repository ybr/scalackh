package ckh.native

case class Block(
  table: Option[String],
  info: BlockInfo,
  nbColumns: Int,
  nbRows: Int,
  columns: List[Column]
)

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