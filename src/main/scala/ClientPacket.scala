sealed trait ClientPacket

import ckh.native.Block

case class ClientInfo(
  name: String,
  version: Version,
  database: String,
  user: String,
  password: String
) extends ClientPacket

case class Query(
  id: Option[String],
  // settings: Map[String, AnyRef],
  stage: QueryProcessingStage,
  compression: Option[CompressionMethod],
  query: String
) extends ClientPacket

case class ClientBlock(block: Block) extends ClientPacket

case object Ping extends ClientPacket