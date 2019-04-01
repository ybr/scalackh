package scalackh.protocol.rw

import scalackh.protocol._
import scalackh.protocol.rw.ColumnDataReaders._
import scalackh.protocol.rw.DefaultReaders._
import scalackh.protocol.rw.LEB128.varIntReader
import scalackh.protocol.rw.PacketTypes.Server._

object ServerPacketReaders {
  val blockInfoReader: Reader[BlockInfo] = Reader { buf =>
    // println(buf)
    var isOverflow: Boolean = false
    var bucketNum: Int = -1

    var fieldNum: Int = 0
    do {
      fieldNum = varIntReader.read(buf) match {
        case Consumed(n) => n
        case NotEnough => throw new IllegalStateException(s"Not enough bytes to read block info")
      }

      if(fieldNum == 1) isOverflow = boolReader.read(buf) match {
        case Consumed(b) => b
        case NotEnough => throw new IllegalStateException(s"Not enough bytes to read block info")
      }
      else if(fieldNum == 2) bucketNum = intReader.read(buf) match {
        case Consumed(b) => b
        case NotEnough => throw new IllegalStateException(s"Not enough bytes to read block info")
      }
    } while (fieldNum != 0)

    Consumed(BlockInfo(isOverflow, bucketNum))
  }

  val serverExceptionReader: Reader[ServerException] = for {
    code <- intReader
    name <- stringReader
    message <- stringReader
    stackTrace <- stringReader
    hasNested <- boolReader
    maybeNested <- {
      if(hasNested) serverExceptionReader.map(Some(_))
      else Reader.pure(None)
    }
  } yield ServerException(code, name, message, stackTrace, maybeNested)

  val blockReader: Reader[Block] = for {
    maybeTableName <- stringReader.map { str =>
      if(str.isEmpty) None
      else Some(str)
    }

    info <- blockInfoReader

    nbColumns <- varIntReader
    nbRows <- varIntReader

    columns <- Reader.traverse((0 until nbColumns).map { _ =>
      for {
        columnName <- stringReader
        columnType <- stringReader
        columnData <- columnDataReader(nbRows, columnType)
      } yield Column(columnName, columnData)
    }.toList)
  } yield Block(maybeTableName, info, nbColumns, nbRows, columns)

  val progressReader: Reader[Progress] = for {
    rows <- varIntReader
    bytes <- varIntReader
    totalRows <- varIntReader.map(Some(_))
  } yield Progress(rows, bytes, totalRows)

  val profileInfoReader: Reader[ProfileInfo] = for {
    rows <- varIntReader
    blocks <- varIntReader
    bytes <- varIntReader
    appliedLimit <- boolReader
    rowsBeforeLimit <- varIntReader
    calculatedRowsBeforeLimit <- boolReader
  } yield ProfileInfo(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit)

  val totalsBlockReader: Reader[TotalsBlock] = blockReader.map(TotalsBlock)

  val extremesBlockReader: Reader[ExtremesBlock] = blockReader.map(ExtremesBlock)

  val logBlockReader: Reader[LogBlock] = blockReader.map(LogBlock)

  val dataBlockReader: Reader[ServerDataBlock] = blockReader.map(ServerDataBlock)

  val protocol: Reader[ServerPacket] = for {
    packetType <- varIntReader
    packet <- packetType match {
      case HELLO => serverInfoReader
      case DATA => dataBlockReader
      case EXCEPTION => serverExceptionReader
      case PROGRESS => progressReader
      case PONG => Reader.pure(Pong)
      case END_OF_STREAM => Reader.pure(EndOfStream)
      case PROFILE_INFO => profileInfoReader
      case TOTALS => totalsBlockReader
      case EXTREMES => extremesBlockReader
      // case TABLES_STATUS_RESPONSE => ???
      case LOG => logBlockReader
      case other => throw new UnsupportedOperationException(s"Unknown server packet type ${other}")
    }
  } yield packet

  val serverInfoReader: Reader[ServerInfo] = for {
    serverName <- stringReader
    major <- varIntReader
    minor <- varIntReader
    revision <- varIntReader
  } yield ServerInfo(serverName, Version(major, minor, revision))
}