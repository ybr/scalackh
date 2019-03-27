package scalackh.protocol.rw

import scalackh.protocol._
import scalackh.protocol.rw.ColumnDataWriters._
import scalackh.protocol.rw.DefaultWriters._
import scalackh.protocol.rw.LEB128.writeVarInt
import scalackh.protocol.rw.PacketTypes.Client._

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

    b.columns.foreach { col =>
      writeString(col.name, buf)

      col.data match {
        case col: DateColumnData => dateColumnDataWriter.write(col, buf)
        case col: DateTimeColumnData => datetimeColumnDataWriter.write(col, buf)
        // case col: Enum8ColumnData => enum8ColumnDataWriter.write(col, buf)
        case col: Float32ColumnData => float32ColumnDataWriter.write(col, buf)
        case col: Float64ColumnData => float64ColumnDataWriter.write(col, buf)
        case col: Int8ColumnData => int8ColumnDataWriter.write(col, buf)
        case col: Int16ColumnData => int16ColumnDataWriter.write(col, buf)
        case col: Int32ColumnData => int32ColumnDataWriter.write(col, buf)
        case col: Int64ColumnData => int64ColumnDataWriter.write(col, buf)
        case col: StringColumnData => stringColumnDataWriter.write(col, buf)
      }
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