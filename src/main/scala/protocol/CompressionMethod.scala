package scalackh.protocol

sealed trait CompressionMethod
case object LZ4 extends CompressionMethod
case object LZ4HC extends CompressionMethod
case object ZSTD extends CompressionMethod