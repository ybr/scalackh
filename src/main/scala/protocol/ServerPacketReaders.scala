import ckh.native._
import ColumnReaders._
import DefaultReaders._
import PacketTypes.Server._
import LEB128._

object ServerPacketReaders {
  val protocol: Reader[ServerPacket] = Reader { buf =>
    val packetType: Int = readVarInt(buf)

    println(s"Packet type ${packetType}")
    val message = packetType match {
      case HELLO => serverInfoReader.read(buf)
      case DATA => blockReader.read(buf)
      case EXCEPTION => serverExceptionReader.read(buf)
      case PROGRESS => progressReader.read(buf)
      case PONG => Pong
      case END_OF_STREAM => EndOfStream
      case PROFILE_INFO => profileInfoReader.read(buf)
      case TOTALS => ???
      case EXTREMES => ???
      case TABLES_STATUS_RESPONSE => ???
      case LOG => ???
      case other => throw new UnsupportedOperationException(s"Unknown server packet type ${packetType}")
    }

    println("Received " + message)
    println(buf)
    message
  }

  val serverInfoReader: Reader[ServerInfo] = Reader { buf =>
    val serverName = readString(buf)
    val major = readVarInt(buf)
    val minor = readVarInt(buf)
    val revision = readVarInt(buf)
    ServerInfo(
      serverName,
      Version(major, minor, revision)
    )
  }

  val blockReader: Reader[ServerBlock] = Reader { buf =>
    val table = readString(buf)

    val info = blockInfoReader.read(buf)

    val nbColumns = readVarInt(buf)

    val nbRows = readVarInt(buf)

    val columns = (0 until nbColumns).map { _ =>
      val columnName = readString(buf)
      val columnType = readString(buf)

      columnType match {
        case "Date" => dateColumnReader(columnName, nbRows).read(buf)
        case "DateTime" => datetimeColumnReader(columnName, nbRows).read(buf)
        case "Float32" => float32ColumnReader(columnName, nbRows).read(buf)
        case "Float64" => float64ColumnReader(columnName, nbRows).read(buf)
        case "Int8" => int8ColumnReader(columnName, nbRows).read(buf)
        case "Int16" => int16ColumnReader(columnName, nbRows).read(buf)
        case "Int32" => int32ColumnReader(columnName, nbRows).read(buf)
        case "Int64" => int64ColumnReader(columnName, nbRows).read(buf)
        case "String" => stringColumnReader(columnName, nbRows).read(buf)
      }
    }.toList

    val block = Block(table, info, nbColumns, nbRows, columns)
    ServerBlock(block)
  }

  val blockInfoReader: Reader[BlockInfo] = Reader { buf =>
    var isOverflow: Boolean = false
    var bucketNum: Int = -1

    var fieldNum: Int = 0
    do {
      fieldNum = readVarInt(buf)

      if(fieldNum == 1) isOverflow = readBool(buf)
      else if(fieldNum == 2) bucketNum = readInt(buf)
    } while (fieldNum != 0)

    BlockInfo(isOverflow, bucketNum)
  }

  val serverExceptionReader: Reader[ServerException] = Reader { buf =>
    val code = readInt(buf)
    val name = readString(buf)
    val message = readString(buf)
    val stackTrace = readString(buf)
    val hasNested = readBool(buf)

    val nested: Option[ServerException] = {
      if(hasNested) Some(serverExceptionReader.read(buf))
      else None
    }

    ServerException(code, name, message, stackTrace, nested)
  }

  val progressReader: Reader[Progress] = Reader { buf =>
    Progress(
      rows = readVarInt(buf),
      bytes = readVarInt(buf),
      totalRows = Some(readVarInt(buf))
    )
  }

  val profileInfoReader: Reader[ProfileInfo] = Reader { buf =>
    val rows = readVarInt(buf)
    val blocks = readVarInt(buf)
    val bytes = readVarInt(buf)
    val appliedLimit = readBool(buf)
    val rowsBeforeLimit = readVarInt(buf)
    val calculatedRowsBeforeLimit = readBool(buf)

    ProfileInfo(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit)
  }
}