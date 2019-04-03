package scalackh.protocol.rw

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
    else Consumed(buf.getShort())
  }

  val intReader: Reader[Int] = Reader { buf =>
    if(buf.remaining < 4) NotEnough
    else Consumed(buf.getInt())
  }

  // val longReader: Reader[Long] = Reader { buf =>
  //   if(buf.remaining < 8) NotEnough
  //   else {
  //     val s = buf.getLong()
  //     Consumed(s)
  //   }
  // }

  // val floatReader: Reader[Float] = Reader { buf =>
  //   if(buf.remaining < 4) NotEnough
  //   else {
  //     val s = buf.getFloat()
  //     Consumed(s)
  //   }
  // }

  // val doubleReader: Reader[Double] = Reader { buf =>
  //   if(buf.remaining < 8) NotEnough
  //   else {
  //     val s = buf.getDouble()
  //     Consumed(s)
  //   }
  // }
}