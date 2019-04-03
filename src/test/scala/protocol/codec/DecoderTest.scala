package scalackh.protocol.codec

import java.nio.{ByteBuffer, ByteOrder}

import minitest._

object DecoderTest extends SimpleTestSuite {
  test("int Decoder consumed 1") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    in.put(data)
    in.flip()

    val result = DefaultDecoders.intDecoder.read(in)

    assertEquals(result, Consumed(1))
    assertEquals(in.position, 4)
  }

  test("int Decoder untouched buffer position on not enough") {
    val data: Array[Byte] = Array(0x01, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    in.put(data)
    in.flip()

    val result = DefaultDecoders.intDecoder.read(in)

    assertEquals(result, NotEnough)
    assertEquals(in.position, 0)
  }

  test("(int, int) Decoder consumed (1, 2)") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    in.put(data)
    in.flip()

    val Decoder: Decoder[(Int, Int)] = for {
      first <- DefaultDecoders.intDecoder
      second <- DefaultDecoders.intDecoder
    } yield (first, second)

    val result = Decoder.read(in)

    assertEquals(Consumed((1, 2)), result)
    assertEquals(in.position, 8)
  }

  test("(int, int) Decoder untouched buffer position on not enough on second int reading") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    in.put(data)
    in.flip()

    val Decoder: Decoder[(Int, Int)] = for {
      first <- DefaultDecoders.intDecoder
      second <- DefaultDecoders.intDecoder
    } yield (first, second)

    val result = Decoder.read(in)

    assertEquals(result, NotEnough)
    assertEquals(in.position, 0)
  }

  test("2 int Decoders the second has not enough bytes position reset to 4") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    in.put(data)
    in.flip()

    val Decoder: Decoder[Int] = DefaultDecoders.intDecoder

    val resultFirst = Decoder.read(in)
    assertEquals(resultFirst, Consumed(1))
    assertEquals(in.position, 4)

    val resultSecond = Decoder.read(in)
    assertEquals(resultSecond, NotEnough)
    assertEquals(in.position, 4)
  }
}