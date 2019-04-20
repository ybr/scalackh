package scalackh.client.core

import java.io._
import java.nio.ByteBuffer

import minitest._

import scalackh.protocol.steps._

object ProtocolAlgTest extends SimpleTestSuite {
  test("bytebuffer contains data read from inputstream") {
    val data: Array[Byte] = Array(
      0x01, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x05, 0x04, 0x64, 0x61, 0x74,
      0x65, 0x04, 0x44, 0x61, 0x74, 0x65, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46, 0x24, 0x46,
      0x06, 0x05, 0x01, 0x0a, 0x00, 0x00, 0x01, 0x03, 0x05, 0x0a, 0x80, 0x40, 0x01, 0x00, 0x01, 0x00,
      0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x05
    ).map(_.toByte)

    val is: InputStream = new ByteArrayInputStream(data)
    val os: OutputStream = new ByteArrayOutputStream()

    val in = ByteBuffer.allocate(1024)
    val out = ByteBuffer.allocate(0)

    ProtocolAlg.iterator(is, os, in, out, NeedsInput.i(_ => Done), true).next

    val (afterPos, afterLimit) = (in.position, in.limit)

    assertEquals(afterPos, 0)

    // buffer shall have been updated
    assertEquals(afterLimit, 57)
  }

  test("needs input from input stream when buffer is not exhausted") {
    val data: Array[Byte] = Array(0x01, 0x02, 0x03, 0x04).map(_.toByte)

    val is: InputStream = new ByteArrayInputStream(data)

    val size = 1024
    val in = ByteBuffer.allocate(size)
    val out = ByteBuffer.allocate(0)

    def step(expected: Int): NeedsInput = NeedsInput { (buf, _) =>
      assertEquals(buf.get().toInt, expected)
      Done
    }

    assertEquals(in.position, 0)
    assertEquals(in.limit, size)

    ProtocolAlg.needsInputStep(is, in, out, step(1), true)
    assertEquals(in.position, 1)
    assertEquals(in.limit, 4)

    is.reset() // input stream receives another data
    ProtocolAlg.needsInputStep(is, in, out, step(2), false)
    assertEquals(in.position, 1)
    assertEquals(in.limit, 7)
  }
}