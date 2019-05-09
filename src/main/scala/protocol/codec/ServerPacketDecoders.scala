package scalackh.protocol.codec

import scalackh.protocol._
import scalackh.protocol.codec.ColumnDataDecoders._
import scalackh.protocol.codec.DefaultDecoders._
import scalackh.protocol.codec.LEB128.varIntDecoder
import scalackh.protocol.codec.PacketTypes.Server._

object ServerPacketDecoders {
  val blockInfoDecoder: Decoder[BlockInfo] = Decoder { buf =>
    var isOverflow: Boolean = false
    var bucketNum: Int = -1

    val fieldDecoder: Decoder[Int] = varIntDecoder.flatMap { fieldNum =>
      val side = {
        if(fieldNum == 1) boolDecoder.map(isOverflow = _)
        else if(fieldNum == 2) intDecoder.map(bucketNum = _)
        else Decoder.pure(())
      }
      side.map (_ => fieldNum)
    }

    def recFieldDecoder: Decoder[Unit] = fieldDecoder.flatMap { fieldNum =>
      if(fieldNum != 0) recFieldDecoder.map(_ => ())
      else Decoder.pure(())
    }

    recFieldDecoder.map(_ => BlockInfo(isOverflow, bucketNum)).read(buf)
  }


  val serverExceptionDecoder: Decoder[ServerException] = for {
    code <- intDecoder
    name <- stringDecoder
    message <- stringDecoder
    stackTrace <- stringDecoder
    hasNested <- boolDecoder
    maybeNested <- {
      if(hasNested) serverExceptionDecoder.map(Some(_))
      else Decoder.pure(None)
    }
  } yield ServerException(code, name, message, stackTrace, maybeNested)

  val blockDecoder: Decoder[Block] = for {
    maybeTableName <- stringDecoder.map { str =>
      if(str.isEmpty) None
      else Some(str)
    }

    info <- blockInfoDecoder

    nbColumns <- varIntDecoder
    nbRows <- varIntDecoder

    columns <- Decoder.traverse((0 until nbColumns).map { _ =>
      for {
        columnName <- stringDecoder
        columnData <- columnDataDecoder(nbRows)
      } yield Column(columnName, columnData)
    }.toList)
  } yield Block(maybeTableName, info, nbColumns, nbRows, columns)

  val progressDecoder: Decoder[Progress] = for {
    rows <- varIntDecoder
    bytes <- varIntDecoder
    totalRows <- varIntDecoder.map(Some(_))
  } yield Progress(rows, bytes, totalRows)

  val profileInfoDecoder: Decoder[ProfileInfo] = for {
    rows <- varIntDecoder
    blocks <- varIntDecoder
    bytes <- varIntDecoder
    appliedLimit <- boolDecoder
    rowsBeforeLimit <- varIntDecoder
    calculatedRowsBeforeLimit <- boolDecoder
  } yield ProfileInfo(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit)

  val totalsBlockDecoder: Decoder[TotalsBlock] = blockDecoder.map(TotalsBlock)

  val extremesBlockDecoder: Decoder[ExtremesBlock] = blockDecoder.map(ExtremesBlock)

  val logBlockDecoder: Decoder[LogBlock] = blockDecoder.map(LogBlock)

  val dataBlockDecoder: Decoder[ServerDataBlock] = blockDecoder.map(ServerDataBlock)

  val protocol: Decoder[ServerPacket] = for {
    packetType <- varIntDecoder
    packet <- packetType match {
      case HELLO => serverInfoDecoder
      case DATA => dataBlockDecoder
      case EXCEPTION => serverExceptionDecoder
      case PROGRESS => progressDecoder
      case PONG => Decoder.pure(Pong)
      case END_OF_STREAM => Decoder.pure(EndOfStream)
      case PROFILE_INFO => profileInfoDecoder
      case TOTALS => totalsBlockDecoder
      case EXTREMES => extremesBlockDecoder
      // case TABLES_STATUS_RESPONSE => ???
      case LOG => logBlockDecoder
      case other => throw new UnsupportedOperationException(s"Unknown server packet type ${other}")
    }
  } yield {
    println("DECODED " + packet)
    packet
  }

  val serverInfoDecoder: Decoder[ServerInfo] = for {
    serverName <- stringDecoder
    major <- varIntDecoder
    minor <- varIntDecoder
    revision <- varIntDecoder
  } yield ServerInfo(serverName, Version(major, minor, revision))
}