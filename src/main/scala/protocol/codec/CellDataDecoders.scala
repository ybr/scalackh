// package scalackh.protocol.codec

// // import java.math.BigInteger
// import java.time.{LocalDate, LocalDateTime, ZoneOffset}
// // import java.util.UUID

// import ckh.native._
// import DefaultDecoders._

// object CellDataDecoders {
//   val tuple = "Tuple\\((.+)\\)".r

//   def dataDecoder(dataType: String): Decoder[CellData] = Decoder { buf =>
//     dataType match {
//       case "Date" => dateDecoder.read(buf)
//       case "DateTime" => datetimeDecoder.read(buf)
//       case "Float32" => float32Decoder.read(buf)
//       case "Float64" => float64Decoder.read(buf)
//       case "Int8" => int8Decoder.read(buf)
//       case "Int16" => int16Decoder.read(buf)
//       case "Int32" => int32Decoder.read(buf)
//       case "Int64" => int64Decoder.read(buf)
//       case "String" => stringDecoder.read(buf)
//       // case tuple(types) => tupleDecoder(types).read(buf)
//       // case "UInt8" => uint8Decoder.read(buf)
//       // case "UInt16" => uint16Decoder.read(buf)
//       // case "UInt32" => uint32Decoder.read(buf)
//       // case "UInt64" => uint64Decoder.read(buf)
//       // case "UUID" => uuidDecoder.read(buf)
//       case other => throw new UnsupportedOperationException(s"Unsupported data type ${other}")
//     }
//   }

//   val dateDecoder: Decoder[DateData] = Decoder { buf =>
//     DateData(LocalDate.ofEpochDay(readShort(buf).toLong))
//   }

//   val datetimeDecoder: Decoder[DateTimeData] = Decoder { buf =>
//     DateTimeData(LocalDateTime.ofEpochSecond(readInt(buf).toLong, 0, ZoneOffset.UTC))
//   }

//   // val enum8Decoder: Decoder[Enum8Data] = Decoder(buf => Enum8Data(buf.get))
//   // val enum16Decoder: Decoder[Enum16Data] = Decoder(buf => Enum16Data(readShort(buf)))

//   val float32Decoder: Decoder[Float32Data] = Decoder(buf => Float32Data(readFloat(buf)))

//   val float64Decoder: Decoder[Float64Data] = Decoder(buf => Float64Data(readDouble(buf)))

//   val int8Decoder: Decoder[Int8Data] = Decoder(buf => Int8Data(buf.get()))

//   val int16Decoder: Decoder[Int16Data] = Decoder(buf => Int16Data(readShort(buf)))

//   val int32Decoder: Decoder[Int32Data] = Decoder(buf => Int32Data(readInt(buf)))

//   val int64Decoder: Decoder[Int64Data] = Decoder(buf => Int64Data(readLong(buf)))

//   val stringDecoder: Decoder[StringData] = Decoder(buf => StringData(readString(buf)))
      
//   // def tupleDecoder(typesStr: String): Decoder[TupleData] = {
//   //   val types = typesStr.split(",").map(_.trim)

//   //   val Decoders: List[Decoder[CellData]] = types.map(dataDecoder).toList

//   //   Decoder(buf => TupleData(Decoders.map(_.read(buf))))
//   // }

//   // val uint8Decoder: Decoder[UInt8Data] = Decoder { buf =>
//   //   UInt8Data((0xff & buf.get()).toShort)
//   // }

//   // val uint16Decoder: Decoder[UInt16Data] = Decoder { buf =>
//   //   UInt16Data((0xffff & readShort(buf)).toInt)
//   // }

//   // val uint32Decoder: Decoder[UInt32Data] = {
//   //   val bytes: Array[Byte] = new Array(4)
//   //   Decoder { buf =>
//   //     buf.get(bytes)

//   //     var result: Long = (bytes(3) & 0xff).toLong
//   //     result = (result << 8) + (bytes(2) & 0xff)
//   //     result = (result << 8) + (bytes(1) & 0xff)
//   //     result = (result << 8) + (bytes(0) & 0xff)

//   //     UInt32Data(result)
//   //   }
//   // }

//   // val uint64Decoder: Decoder[UInt64Data] = {
//   //   val bytes: Array[Byte] = new Array(8)
//   //   Decoder { buf =>
//   //     bytes(7) = buf.get()
//   //     bytes(6) = buf.get()
//   //     bytes(5) = buf.get()
//   //     bytes(4) = buf.get()
//   //     bytes(3) = buf.get()
//   //     bytes(2) = buf.get()
//   //     bytes(1) = buf.get()
//   //     bytes(0) = buf.get()

//   //     UInt64Data(new BigInteger(bytes))
//   //   }
//   // }

//   // val uuidDecoder: Decoder[UuidData] = Decoder { buf =>
//   //   val mostSigBits: Long = readLong(buf)
//   //   val leastSigBits: Long = readLong(buf)
//   //   UuidData(new UUID(mostSigBits, leastSigBits))
//   // }
// }