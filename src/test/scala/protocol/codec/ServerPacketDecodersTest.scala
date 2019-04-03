package scalackh.protocol.codec

import scalackh.protocol.BlockInfo

import java.nio.{ByteBuffer, ByteOrder}

import minitest._

object ServerPacketDecodersTest extends SimpleTestSuite {
  test("Consumed BlockInfo(true,10)") {
    val in = ByteBuffer.wrap(Array(0x01, 0x01, 0x02, 0x0a, 0x00, 0x00, 0x00, 0x00).map(_.toByte)).order(ByteOrder.LITTLE_ENDIAN)
    val Consumed(result) = ServerPacketDecoders.blockInfoDecoder.read(in)
    assertEquals(result, BlockInfo(true, 10))
  }

  test("Consumed field isOverflow BlockInfo(true,-1)") {
    val in = ByteBuffer.wrap(Array(0x01, 0x01, 0x00).map(_.toByte)).order(ByteOrder.LITTLE_ENDIAN)
    val Consumed(result) = ServerPacketDecoders.blockInfoDecoder.read(in)
    assertEquals(result, BlockInfo(true, -1))
  }

  test("Consumed field bucketNum BlockInfo(false,3)") {
    val in = ByteBuffer.wrap(Array(0x02, 0x03, 0x00, 0x00, 0x00, 0x00).map(_.toByte)).order(ByteOrder.LITTLE_ENDIAN)
    val Consumed(result) = ServerPacketDecoders.blockInfoDecoder.read(in)
    assertEquals(result, BlockInfo(false, 3))
  }

  test("Consumed no field BlockInfo(false,-1)") {
    val in = ByteBuffer.wrap(Array(0x00).map(_.toByte)).order(ByteOrder.LITTLE_ENDIAN)
    val Consumed(result) = ServerPacketDecoders.blockInfoDecoder.read(in)
    assertEquals(result, BlockInfo(false, -1))
  }

  test("NotEnough field bucketNum BlockInfo(false,-1)") {
    val in = ByteBuffer.wrap(Array(0x02, 0x03, 0x00).map(_.toByte)).order(ByteOrder.LITTLE_ENDIAN)
    val result = ServerPacketDecoders.blockInfoDecoder.read(in)
    assertEquals(result, NotEnough)
  }
}