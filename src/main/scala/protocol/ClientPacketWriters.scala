import ckh.native._

import DefaultWriters._
import PacketTypes.Client._
import LEB128._

object ClientPacketWriters {
  val message: Writer[ClientPacket] = Writer { (m, buf) =>
    println("Sending " + m + "...")
    m match {
      case info: ClientInfo => clientInfoWriter.write(info, buf)
      case q: Query => queryWriter.write(q, buf)
      case b: ClientBlock => blockWriter.write(b, buf)
      case Ping => pingWriter.write(Ping, buf)
    }
  }

  val clientInfoWriter: Writer[ClientInfo] = Writer { (ci, buf) =>
    writeVarInt(HELLO, buf)
    writeString(ci.name, buf)
    writeVarInt(ci.version.major, buf)
    writeVarInt(ci.version.minor, buf)
    writeVarInt(ci.version.revision, buf)
    writeString(ci.database, buf)
    writeString(ci.user, buf)
    writeString(ci.password, buf)
  }

  val queryWriter: Writer[Query] = Writer { (q, buf) =>
    writeVarInt(QUERY, buf)
    writeString(q.id.getOrElse(""), buf)
    // buf.put(1.toByte)
    settingsWriter.write((), buf)
    stageWriter.write(q.stage, buf)
    writeVarInt(0, buf) // compression TODO
    writeString(q.query, buf)
  }

  val blockWriter: Writer[ClientBlock] = Writer { (cb, buf) =>
    writeVarInt(DATA, buf) // message type

    val block = cb.block

    writeString(block.table, buf)
    blockInfoWriter.write(block.info, buf)

    writeVarInt(block.nbColumns, buf)
    writeVarInt(block.nbRows, buf)
  }

  val blockInfoWriter: Writer[BlockInfo] = Writer { (bi, buf) =>
    writeVarInt(1, buf) // field num for isOverflow
    writeBool(bi.isOverflow, buf)
    writeVarInt(2, buf) // field num for bucket num
    writeInt(bi.bucketNum, buf)
    writeVarInt(0, buf) // end of fields
  }

  val pingWriter: Writer[Ping.type] = Writer { (_, buf) =>
    writeVarInt(PING, buf)
  }

  // it should be something like a dictionary
  val settingsWriter: Writer[Unit] = Writer { (_, buf) =>
    // writeString("extremes", buf)
    // buf.put(0.toByte)
    writeString("", buf) // end of settings
  }

  val stageWriter: Writer[QueryProcessingStage] = Writer { (s, buf) =>
    val int: Int = s match {
      case FetchColumns => 0
      case WithMergeableState => 1
      case Complete => 2
    }
    writeVarInt(int, buf)
  }
}