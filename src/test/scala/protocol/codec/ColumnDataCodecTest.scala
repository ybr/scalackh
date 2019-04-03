package scalackh.protocol.codec

import scalackh.protocol._

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import org.scalacheck._
import org.scalacheck.Prop.forAll

object ColumnDataCodecTest extends Properties("Codec ColumnData") {
  property("Date") = forAll(Gen.listOfN(10, Gen.chooseNum(Short.MinValue, Short.MaxValue).map(short => LocalDate.ofEpochDay(short.toLong)))) { (l1: List[LocalDate]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.dateColumnDataEncoder.write(DateColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(DateColumnData(l2)) = ColumnDataDecoders.dateColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("DateTime") = forAll(Gen.listOfN(10, Gen.chooseNum(Int.MinValue, Int.MaxValue).map(int => LocalDateTime.ofEpochSecond(int.toLong, 0, ZoneOffset.UTC)))) { (l1: List[LocalDateTime]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.dateTimeColumnDataEncoder.write(DateTimeColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(DateTimeColumnData(l2)) = ColumnDataDecoders.dateTimeColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("String") = forAll(Gen.listOfN(10, Gen.asciiStr)) { (l1: List[String]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.stringColumnDataEncoder.write(StringColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(StringColumnData(l2)) = ColumnDataDecoders.stringColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("FixedString") = forAll(Gen.listOfN(10, Gen.asciiStr.filter(_.length > 16).map(_.take(16)))) { (l1: List[String]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.fixedStringColumnDataEncoder.write(FixedStringColumnData(16, l1.toArray), buf)
    buf.position(0)
    val Consumed(FixedStringColumnData(16, l2)) = ColumnDataDecoders.fixedStringColumnDataDecoder(16, l1.length).read(buf)

    l1 == l2.toList
  }

  property("Float64") = forAll(Gen.listOfN(10, Gen.chooseNum(Double.NegativeInfinity, Double.PositiveInfinity))) { (l1: List[Double]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.float64ColumnDataEncoder.write(Float64ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Float64ColumnData(l2)) = ColumnDataDecoders.float64ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("Float32") = forAll(Gen.listOfN(10, Gen.chooseNum(Float.NegativeInfinity, Float.PositiveInfinity))) { (l1: List[Float]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.float32ColumnDataEncoder.write(Float32ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Float32ColumnData(l2)) = ColumnDataDecoders.float32ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("Int8") = forAll(Gen.listOfN(10, Gen.chooseNum(Byte.MinValue, Byte.MaxValue))) { (l1: List[Byte]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.int8ColumnDataEncoder.write(Int8ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Int8ColumnData(l2)) = ColumnDataDecoders.int8ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("Int16") = forAll(Gen.listOfN(10, Gen.chooseNum(Short.MinValue, Short.MaxValue))) { (l1: List[Short]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.int16ColumnDataEncoder.write(Int16ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Int16ColumnData(l2)) = ColumnDataDecoders.int16ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("Int32") = forAll(Gen.listOfN(10, Gen.chooseNum(Int.MinValue, Int.MaxValue))) { (l1: List[Int]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.int32ColumnDataEncoder.write(Int32ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Int32ColumnData(l2)) = ColumnDataDecoders.int32ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("Int64") = forAll(Gen.listOfN(10, Gen.chooseNum(Long.MinValue, Long.MaxValue))) { (l1: List[Long]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.int64ColumnDataEncoder.write(Int64ColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(Int64ColumnData(l2)) = ColumnDataDecoders.int64ColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }

  property("UUID") = forAll(Gen.listOfN(10, Gen.uuid)) { (l1: List[UUID]) =>
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

    ColumnDataEncoders.uuidColumnDataEncoder.write(UuidColumnData(l1.toArray), buf)
    buf.position(0)
    val Consumed(UuidColumnData(l2)) = ColumnDataDecoders.uuidColumnDataDecoder(l1.length).read(buf)

    l1 == l2.toList
  }
}