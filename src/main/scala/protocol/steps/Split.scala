package scalackh.protocol.steps

import scalackh.protocol._

object Split {
  def splitColumn(maxSize: Int)(colData: ColumnData): (ColumnData, ColumnData) = colData match {
    case DateColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      DateColumnData(dataMaxSize) -> DateColumnData(dataRemaining)
    case DateTimeColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      DateTimeColumnData(dataMaxSize) -> DateTimeColumnData(dataRemaining)

    case Enum8ColumnData(enums, data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Enum8ColumnData(enums, dataMaxSize) -> Enum8ColumnData(enums, dataRemaining)
    case Enum16ColumnData(enums, data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Enum16ColumnData(enums, dataMaxSize) -> Enum16ColumnData(enums, dataRemaining)


    case FixedStringColumnData(strLength, data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      FixedStringColumnData(strLength, dataMaxSize) -> FixedStringColumnData(strLength, dataRemaining)

    case Float32ColumnData(data) => 
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Float32ColumnData(dataMaxSize) -> Float32ColumnData(dataRemaining)
    case Float64ColumnData(data) => 
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Float64ColumnData(dataMaxSize) -> Float64ColumnData(dataRemaining)

    case Int8ColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Int8ColumnData(dataMaxSize) -> Int8ColumnData(dataRemaining)
    case Int16ColumnData(data) => 
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Int16ColumnData(dataMaxSize) -> Int16ColumnData(dataRemaining)
    case Int32ColumnData(data) => 
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Int32ColumnData(dataMaxSize) -> Int32ColumnData(dataRemaining)
    case Int64ColumnData(data) => 
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      Int64ColumnData(dataMaxSize) -> Int64ColumnData(dataRemaining)

    case NullableColumnData(nulls, data) =>
      val (nullsMaxSize, nullsRemaining) = nulls.splitAt(maxSize)
      val (colDataMaxSize, colDataRemaining) = splitColumn(maxSize)(data)
      NullableColumnData(nullsMaxSize, colDataMaxSize) -> NullableColumnData(nullsRemaining, colDataRemaining)

    case StringColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      StringColumnData(dataMaxSize) -> StringColumnData(dataRemaining)

    case UuidColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      UuidColumnData(dataMaxSize) -> UuidColumnData(dataRemaining)
  }

  // returns first block with nbRows less than or equals to maxSize, second block the remaining
  def splitBlock(maxSize: Int)(block: Block): (Block, Block) = {
    val (maxSizeColumns, remainingColumns) = block.columns.map { col =>
      val (colDataMaxSize, colDataRemaining) = splitColumn(maxSize)(col.data)
      Column(col.name, colDataMaxSize) -> Column(col.name, colDataRemaining)
    }.unzip

    val nbRowsNotMoved = Math.max(0, block.nbRows - maxSize)

    val maxSizeBlock = block.copy(columns = maxSizeColumns, nbRows = block.nbRows - nbRowsNotMoved)
    val remainingBlock = block.copy(columns = remainingColumns, nbRows = nbRowsNotMoved)
    (maxSizeBlock, remainingBlock)
  }
}