package scalackh.protocol.codec

import scalackh.protocol._
import scalackh.protocol.codec.ColumnDataEncoders._
import scalackh.protocol.codec.DefaultEncoders._
import scalackh.protocol.codec.LEB128.writeVarInt
import scalackh.protocol.codec.PacketTypes.Client._

object ClientPacketEncoders {
  val message: Encoder[ClientPacket] = Encoder { (m, buf) =>
    m match {
      case info: ClientInfo => clientInfoEncoder.write(info, buf)
      case q: Query => queryEncoder.write(q, buf)
      case db: ClientDataBlock => clientDataBlockEncoder.write(db, buf)
      case Cancel => writeVarInt(CANCEL, buf)
      case Ping => writeVarInt(PING, buf)
    }
  }

  val clientInfoEncoder: Encoder[ClientInfo] = Encoder { (ci, buf) =>
    writeVarInt(HELLO, buf)
    writeString(ci.name, buf)
    writeVarInt(ci.version.major, buf)
    writeVarInt(ci.version.minor, buf)
    writeVarInt(ci.version.revision, buf)
    writeString(ci.database, buf)
    writeString(ci.user, buf)
    writeString(ci.password, buf)
  }

  val queryEncoder: Encoder[Query] = Encoder { (q, buf) =>
    writeVarInt(QUERY, buf)
    writeString(q.id.getOrElse(""), buf)
    settingsEncoder.write((), buf)
    stageEncoder.write(q.stage, buf)
    writeVarInt(0, buf) // compression TODO
    writeString(q.query, buf)
  }

  val clientDataBlockEncoder: Encoder[ClientDataBlock] = Encoder { (cb, buf) =>
    writeVarInt(DATA, buf)
    blockEncoder.write(cb.block, buf)
  }

  val blockEncoder: Encoder[Block] = Encoder { (b, buf) =>
    writeString(b.table.getOrElse(""), buf)
    blockInfoEncoder.write(b.info, buf)
    writeVarInt(b.nbColumns, buf)
    writeVarInt(b.nbRows, buf)

    b.columns.foreach { col =>
      writeString(col.name, buf)
      columnDataEncoder.write(col.data, buf)
    }
  }

  val blockInfoEncoder: Encoder[BlockInfo] = Encoder { (bi, buf) =>
    writeVarInt(1, buf) // field num for isOverflow
    writeBool(bi.isOverflow, buf)
    writeVarInt(2, buf) // field num for bucket num
    buf.putInt(bi.bucketNum)
    writeVarInt(0, buf) // end of fields
  }

  // it should be something like a dictionary
  val settingsEncoder: Encoder[Unit] = Encoder { (_, buf) =>
    // writeString("extremes", buf)
    // buf.put(0.toByte)
    writeString("", buf) // end of settings
  }

  val stageEncoder: Encoder[QueryProcessingStage] = Encoder { (s, buf) =>
    val int: Int = s match {
      case FetchColumns => 0
      case WithMergeableState => 1
      case Complete => 2
    }
    writeVarInt(int, buf)
  }
}