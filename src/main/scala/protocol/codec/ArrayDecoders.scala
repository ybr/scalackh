// package scalackh.protocol.codec

// import java.time.{LocalDate, LocalDateTime, ZoneOffset}

// import ckh.native._
// import DefaultDecoders._

// object ArrayDecoders {
//   def arrayDecoder(nbRows: Int, elemType: String): Decoder[ClickhouseArray] = Decoder { buf =>
//     val sizes = arraySizesDecoder(nbRows).read(buf)

//     elemType match {
//       case "Date" => arrayDateDecoder(nbRows, sizes).read(buf)
//       case "DateTime" => arrayDateTimeDecoder(nbRows, sizes).read(buf)
//       case "Float32" => arrayFloat32Decoder(nbRows, sizes).read(buf)
//       case "Float64" => arrayFloat64Decoder(nbRows, sizes).read(buf)
//       case "Int8" => arrayInt8Decoder(nbRows, sizes).read(buf)
//       case "Int16" => arrayInt16Decoder(nbRows, sizes).read(buf)
//       case "Int32" => arrayInt32Decoder(nbRows, sizes).read(buf)
//       case "Int64" => arrayInt64Decoder(nbRows, sizes).read(buf)
//       case "String" => arrayStringDecoder(nbRows, sizes).read(buf)
//       // case "UInt8" => arrayUInt8Decoder(nbRows, sizes).read(buf)
//       // case "Int16" => arrayUInt16Decoder(nbRows, sizes).read(buf)
//       // case "Int32" => arrayUInt32Decoder(nbRows, sizes).read(buf)
//       // case "Int64" => arrayUInt64Decoder(nbRows, sizes).read(buf)
//     }
//   }

//   // manages only 1-d arrays
//   def arraySizesDecoder(nbRows: Int): Decoder[Array[Int]] = Decoder { buf =>
//     // From https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/columns/arraycolumn.py
//     // Nested arrays written in flatten form after information about their
//     // sizes (offsets really).
//     // One element of array of arrays can be represented as tree:
//     // (0 depth)          [[3, 4], [5, 6]]
//     //                   |               |
//     // (1 depth)      [3, 4]           [5, 6]
//     //                |    |           |    |
//     // (leaf)        3     4          5     6
//     // Offsets (sizes) written in breadth-first search order. In example above
//     // following sequence of offset will be written: 4 -> 2 -> 4
//     // 1) size of whole array: 4
//     // 2) size of array 1 in depth=1: 2
//     // 3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4
//     // After sizes info comes flatten data: 3 -> 4 -> 5 -> 6

//     val sizes: Array[Int] = new Array[Int](nbRows)

//     var i: Int = 0
//     var currentOffset: Int = 0

//     while(i < nbRows) {
//       val nextOffset = readLong(buf).toInt
//       sizes(i) = nextOffset - currentOffset
//       currentOffset = nextOffset
//       i = i + 1
//     }

//     sizes
//   }

//   def arrayDateDecoder(nbRows: Int, sizes: Array[Int]): Decoder[DateArray] = Decoder { buf =>
//     val data: Array[Array[LocalDate]] = new Array[Array[LocalDate]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[LocalDate](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = LocalDate.ofEpochDay(readShort(buf).toLong)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     DateArray(data)
//   }

//   def arrayDateTimeDecoder(nbRows: Int, sizes: Array[Int]): Decoder[DateTimeArray] = Decoder { buf =>
//     val data: Array[Array[LocalDateTime]] = new Array[Array[LocalDateTime]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[LocalDateTime](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = LocalDateTime.ofEpochSecond(readInt(buf).toLong, 0, ZoneOffset.UTC)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     DateTimeArray(data)
//   }

//   def arrayFloat64Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Float64Array] = Decoder { buf =>
//     val data: Array[Array[Double]] = new Array[Array[Double]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Double](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readDouble(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Float64Array(data)
//   }

//   def arrayFloat32Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Float32Array] = Decoder { buf =>
//     val data: Array[Array[Float]] = new Array[Array[Float]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Float](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readFloat(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Float32Array(data)
//   }

//   def arrayInt8Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Int8Array] = Decoder { buf =>
//     val data: Array[Array[Byte]] = new Array[Array[Byte]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Byte](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = buf.get()
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Int8Array(data)
//   }

//   def arrayInt16Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Int16Array] = Decoder { buf =>
//     val data: Array[Array[Short]] = new Array[Array[Short]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Short](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readShort(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Int16Array(data)
//   }

//   def arrayInt32Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Int32Array] = Decoder { buf =>
//     val data: Array[Array[Int]] = new Array[Array[Int]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Int](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readInt(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Int32Array(data)
//   }

//   def arrayInt64Decoder(nbRows: Int, sizes: Array[Int]): Decoder[Int64Array] = Decoder { buf =>
//     val data: Array[Array[Long]] = new Array[Array[Long]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[Long](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readLong(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     Int64Array(data)
//   }

//   def arrayStringDecoder(nbRows: Int, sizes: Array[Int]): Decoder[StringArray] = Decoder { buf =>
//     val data: Array[Array[String]] = new Array[Array[String]](nbRows)

//     var i: Int = 0
//     while(i < nbRows) {
//       val arraySize: Int = sizes(i)
//       val arr = new Array[String](arraySize)
//       data(i) = arr

//       var j: Int = 0
//       while(j < arraySize) {
//         arr(j) = readString(buf)
//         j = j + 1
//       }

//       i = i + 1
//     }

//     StringArray(data)
//   }
// }