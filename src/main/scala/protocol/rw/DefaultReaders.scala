package scalackh.protocol.rw

import java.nio.{ByteBuffer, ByteOrder}

import scalackh.protocol.rw.LEB128.varIntReader

object DefaultReaders {
  val boolReader: Reader[Boolean] = Reader { buf =>
    if(buf.remaining == 0) NotEnough
    else Consumed(buf.get() != 0)
  }

  def bytesFixedReader(length: Int): Reader[Array[Byte]] = Reader { buf =>
    if(buf.remaining < length) NotEnough
    else {
      val bytesFixedLength = new Array[Byte](length)
      buf.get(bytesFixedLength)
      Consumed(bytesFixedLength)
    }
  }

  val bytesReader: Reader[Array[Byte]] = for {
    length <- varIntReader
    bytes <- bytesFixedReader(length)
  } yield bytes

  def stringFixedReader(length: Int): Reader[String] = bytesFixedReader(length).map(new String(_, "UTF-8"))

  val stringReader: Reader[String] = bytesReader.map(new String(_, "UTF-8"))

  val shortReader: Reader[Short] = Reader { buf =>
    if(buf.remaining < 1) NotEnough
    else {
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val s = buf.getShort()
      buf.order(ByteOrder.BIG_ENDIAN)
      Consumed(s)
    }
  }

  val intReader: Reader[Int] = Reader { buf =>
    if(buf.remaining < 4) NotEnough
    else {
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val int = buf.getInt()
      buf.order(ByteOrder.BIG_ENDIAN)
      Consumed(int)
    }
  }

  val longReader: Reader[Long] = Reader { buf =>
    if(buf.remaining < 8) NotEnough
    else {
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val s = buf.getLong()
      buf.order(ByteOrder.BIG_ENDIAN)
      Consumed(s)
    }
  }

  val floatReader: Reader[Float] = Reader { buf =>
    if(buf.remaining < 4) NotEnough
    else {
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val s = buf.getFloat()
      buf.order(ByteOrder.BIG_ENDIAN)
      Consumed(s)
    }
  }

  val doubleReader: Reader[Double] = Reader { buf =>
    if(buf.remaining < 8) NotEnough
    else {
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val s = buf.getDouble()
      buf.order(ByteOrder.BIG_ENDIAN)
      Consumed(s)
    }
  }

  def readShort(buf: ByteBuffer): Short = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getShort()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readInt(buf: ByteBuffer): Int = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val int = buf.getInt()
    buf.order(ByteOrder.BIG_ENDIAN)
    int
  }

  def readLong(buf: ByteBuffer): Long = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getLong()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readFloat(buf: ByteBuffer): Float = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getFloat()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }

  def readDouble(buf: ByteBuffer): Double = {
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val s = buf.getDouble()
    buf.order(ByteOrder.BIG_ENDIAN)
    s
  }
}