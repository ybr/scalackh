package scalackh.protocol.steps

import scalackh.protocol._

object Split {
  def splitColumn(maxSize: Int)(colData: ColumnData): (ColumnData, ColumnData) = colData match {
    case ArrayColumnData(data) =>
      data match {
        case DateArray(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(DateArray(dataMaxSize)) -> ArrayColumnData(DateArray(dataRemaining))
        case DateTimeArray(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(DateTimeArray(dataMaxSize)) -> ArrayColumnData(DateTimeArray(dataRemaining))
        case Float32Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Float32Array(dataMaxSize)) -> ArrayColumnData(Float32Array(dataRemaining))
        case Float64Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Float64Array(dataMaxSize)) -> ArrayColumnData(Float64Array(dataRemaining))
        case Int8Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Int8Array(dataMaxSize)) -> ArrayColumnData(Int8Array(dataRemaining))
        case Int16Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Int16Array(dataMaxSize)) -> ArrayColumnData(Int16Array(dataRemaining))
        case Int32Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Int32Array(dataMaxSize)) -> ArrayColumnData(Int32Array(dataRemaining))
        case Int64Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(Int64Array(dataMaxSize)) -> ArrayColumnData(Int64Array(dataRemaining))
        case StringArray(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(StringArray(dataMaxSize)) -> ArrayColumnData(StringArray(dataRemaining))
        case UInt8Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(UInt8Array(dataMaxSize)) -> ArrayColumnData(UInt8Array(dataRemaining))
        case UInt16Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(UInt16Array(dataMaxSize)) -> ArrayColumnData(UInt16Array(dataRemaining))
        case UInt32Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(UInt32Array(dataMaxSize)) -> ArrayColumnData(UInt32Array(dataRemaining))
        case UInt64Array(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(UInt64Array(dataMaxSize)) -> ArrayColumnData(UInt64Array(dataRemaining))
        case UuidArray(arrayData) =>
          val (dataMaxSize, dataRemaining) = arrayData.splitAt(maxSize)
          ArrayColumnData(UuidArray(dataMaxSize)) -> ArrayColumnData(UuidArray(dataRemaining))
      }

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

    case UInt8ColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      UInt8ColumnData(dataMaxSize) -> UInt8ColumnData(dataRemaining)
    case UInt16ColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      UInt16ColumnData(dataMaxSize) -> UInt16ColumnData(dataRemaining)
    case UInt32ColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      UInt32ColumnData(dataMaxSize) -> UInt32ColumnData(dataRemaining)
    case UInt64ColumnData(data) =>  
      val (dataMaxSize, dataRemaining) = data.splitAt(maxSize)
      UInt64ColumnData(dataMaxSize) -> UInt64ColumnData(dataRemaining)

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