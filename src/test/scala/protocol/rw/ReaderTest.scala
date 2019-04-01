package scalackh.protocol.rw

import java.nio.ByteBuffer

import minitest._

object ReaderTest extends SimpleTestSuite {
  test("int reader consumed 1") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16)
    in.put(data)
    in.flip()

    val result = DefaultReaders.intReader.read(in)

    assertEquals(result, Consumed(1))
    assertEquals(in.position, 4)
  }

  test("int reader untouched buffer position on not enough") {
    val data: Array[Byte] = Array(0x01, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16)
    in.put(data)
    in.flip()

    val result = DefaultReaders.intReader.read(in)

    assertEquals(result, NotEnough)
    assertEquals(in.position, 0)
  }

  test("(int, int) reader consumed (1, 2)") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16)
    in.put(data)
    in.flip()

    val reader: Reader[(Int, Int)] = for {
      first <- DefaultReaders.intReader
      second <- DefaultReaders.intReader
    } yield (first, second)

    val result = reader.read(in)

    assertEquals(Consumed((1, 2)), result)
    assertEquals(in.position, 8)
  }

  test("(int, int) reader untouched buffer position on not enough on second int reading") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16)
    in.put(data)
    in.flip()

    val reader: Reader[(Int, Int)] = for {
      first <- DefaultReaders.intReader
      second <- DefaultReaders.intReader
    } yield (first, second)

    val result = reader.read(in)

    assertEquals(result, NotEnough)
    assertEquals(in.position, 0)
  }

  test("2 int readers the second has not enough bytes position reset to 4") {
    val data: Array[Byte] = Array(0x01, 0x00, 0x00, 0x00, 0x02, 0x00).map(_.toByte)

    val in = ByteBuffer.allocate(16)
    in.put(data)
    in.flip()

    val reader: Reader[Int] = DefaultReaders.intReader

    val resultFirst = reader.read(in)
    assertEquals(resultFirst, Consumed(1))
    assertEquals(in.position, 4)

    val resultSecond = reader.read(in)
    assertEquals(resultSecond, NotEnough)
    assertEquals(in.position, 4)
  }
}