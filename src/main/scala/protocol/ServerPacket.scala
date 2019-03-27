package scalackh.protocol

sealed trait ServerPacket

case class ServerInfo(
  name: String,
  version: Version
) extends ServerPacket

case class ServerDataBlock(block: Block) extends ServerPacket

case class ServerException(
  code: Int,
  name: String,
  message: String,
  stackTrace: String,
  nested: Option[ServerPacket]
) extends ServerPacket

case class Progress(
  rows: Int,
  bytes: Int,
  totalRows: Option[Int]
) extends ServerPacket

case object Pong extends ServerPacket

case object EndOfStream extends ServerPacket

case class ProfileInfo(
  rows: Int,
  blocks: Int,
  bytes: Int,
  appliedLimit: Boolean,
  rowsBeforeLimit: Int,
  calculatedRowsBeforeLimit: Boolean
) extends ServerPacket

case class TotalsBlock(block: Block) extends ServerPacket

case class ExtremesBlock(block: Block) extends ServerPacket

case class LogBlock(block: Block) extends ServerPacket