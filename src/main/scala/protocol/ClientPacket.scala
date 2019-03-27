package scalackh.protocol

sealed trait ClientPacket

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

case class ClientDataBlock(block: Block) extends ClientPacket

case object Cancel extends ClientPacket

case object Ping extends ClientPacket