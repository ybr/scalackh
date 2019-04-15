package scalackh.protocol.codec

import scalackh.protocol._

import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util.UUID

import org.scalacheck._
import org.scalacheck.Prop.forAll

import scala.reflect.ClassTag

object ArrayCodecTest extends Properties("Codec Array") {
  def arrayGen[T](maxSize: Int)(elemGen: Gen[T])(implicit ct: ClassTag[T]): Gen[Array[T]] = for {
    arraySize <- Gen.chooseNum(0, maxSize)
    list <- Gen.listOfN(arraySize, elemGen)
  } yield list.toArray

  property("Date") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(0, Short.MaxValue).map(b => LocalDate.ofEpochDay(b.toLong))))) { (arrays: Array[Array[LocalDate]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(DateArray(arrays), buf)
    buf.position(0)

    val Consumed(DateArray(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Date").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("DateTime") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(0, Int.MaxValue).map(n => LocalDateTime.ofEpochSecond(n.toLong, 0, ZoneOffset.UTC))))) { (arrays: Array[Array[LocalDateTime]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(DateTimeArray(arrays), buf)
    buf.position(0)

    val Consumed(DateTimeArray(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "DateTime").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Float32") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Float.MinValue, Float.MaxValue)))) { (arrays: Array[Array[Float]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Float32Array(arrays), buf)
    buf.position(0)

    val Consumed(Float32Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Float32").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Float64") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Double.MinValue, Double.MaxValue)))) { (arrays: Array[Array[Double]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Float64Array(arrays), buf)
    buf.position(0)

    val Consumed(Float64Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Float64").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Int8") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Byte.MinValue, Byte.MaxValue)))) { (arrays: Array[Array[Byte]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Int8Array(arrays), buf)
    buf.position(0)

    val Consumed(Int8Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Int8").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Int16") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Short.MinValue, Short.MaxValue)))) { (arrays: Array[Array[Short]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Int16Array(arrays), buf)
    buf.position(0)

    val Consumed(Int16Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Int16").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Int32") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Int.MinValue, Int.MaxValue)))) { (arrays: Array[Array[Int]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Int32Array(arrays), buf)
    buf.position(0)

    val Consumed(Int32Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Int32").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("Int64") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Long.MinValue, Long.MaxValue)))) { (arrays: Array[Array[Long]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(Int64Array(arrays), buf)
    buf.position(0)

    val Consumed(Int64Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "Int64").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("String") = forAll(arrayGen(50)(arrayGen(50)(Gen.asciiStr))) { (arrays: Array[Array[String]]) =>
    val buf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(StringArray(arrays), buf)
    buf.position(0)

    val Consumed(StringArray(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "String").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("UInt8") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Byte.MinValue, Byte.MaxValue)))) { (arrays: Array[Array[Byte]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(UInt8Array(arrays), buf)
    buf.position(0)

    val Consumed(UInt8Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "UInt8").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("UInt16") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Short.MinValue, Short.MaxValue)))) { (arrays: Array[Array[Short]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(UInt16Array(arrays), buf)
    buf.position(0)

    val Consumed(UInt16Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "UInt16").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("UInt32") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Int.MinValue, Int.MaxValue)))) { (arrays: Array[Array[Int]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(UInt32Array(arrays), buf)
    buf.position(0)

    val Consumed(UInt32Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "UInt32").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("UInt64") = forAll(arrayGen(50)(arrayGen(50)(Gen.chooseNum(Long.MinValue, Long.MaxValue)))) { (arrays: Array[Array[Long]]) =>
    val buf = ByteBuffer.allocate(16384).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(UInt64Array(arrays), buf)
    buf.position(0)

    val Consumed(UInt64Array(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "UInt64").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }

  property("UUID") = forAll(arrayGen(50)(arrayGen(50)(Gen.uuid))) { (arrays: Array[Array[UUID]]) =>
    val buf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)

    ArrayEncoders.arrayEncoder.write(UuidArray(arrays), buf)
    buf.position(0)

    val Consumed(UuidArray(decoded)) = ArrayDecoders.arrayDecoder(arrays.length, "UUID").read(buf)

    val expected = arrays.map(_.toList).toList
    val actual = decoded.map(_.toList).toList

    actual == expected
  }
}