package ckh.protocol

import ckh.native._

import ColumnWriters._
import DefaultWriters._
import PacketTypes.Client._
import LEB128._

object ClientPacketWriters {
  val message: Writer[ClientPacket] = Writer { (m, buf) =>
    m match {
      case info: ClientInfo => clientInfoWriter.write(info, buf)
      case q: Query => queryWriter.write(q, buf)
      case db: ClientDataBlock => clientDataBlockWriter.write(db, buf)
      case Cancel => writeVarInt(CANCEL, buf)
      case Ping => writeVarInt(PING, buf)
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

  val clientDataBlockWriter: Writer[ClientDataBlock] = Writer { (cb, buf) =>
    writeVarInt(DATA, buf)
    blockWriter.write(cb.block, buf)
  }

  val blockWriter: Writer[Block] = Writer { (b, buf) =>
    writeString(b.table.getOrElse(""), buf)
    blockInfoWriter.write(b.info, buf)
    writeVarInt(b.nbColumns, buf)
    writeVarInt(b.nbRows, buf)

    b.columns.foreach {
      case col: DateColumn => dateColumnWriter.write(col, buf)
      case col: DateTimeColumn => datetimeColumnWriter.write(col, buf)
      case col: Float32Column => float32ColumnWriter.write(col, buf)
      case col: Float64Column => float64ColumnWriter.write(col, buf)
      case col: Int8Column => int8ColumnWriter.write(col, buf)
      case col: Int16Column => int16ColumnWriter.write(col, buf)
      case col: Int32Column => int32ColumnWriter.write(col, buf)
      case col: Int64Column => int64ColumnWriter.write(col, buf)
      case col: StringColumn => stringColumnWriter.write(col, buf)
    }
  }

  val blockInfoWriter: Writer[BlockInfo] = Writer { (bi, buf) =>
    writeVarInt(1, buf) // field num for isOverflow
    writeBool(bi.isOverflow, buf)
    writeVarInt(2, buf) // field num for bucket num
    writeInt(bi.bucketNum, buf)
    writeVarInt(0, buf) // end of fields
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