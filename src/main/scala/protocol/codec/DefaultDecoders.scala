package scalackh.protocol.codec

import scalackh.protocol.codec.LEB128.varIntDecoder

object DefaultDecoders {
  val boolDecoder: Decoder[Boolean] = Decoder { buf =>
    if(buf.remaining == 0) NotEnough
    else Consumed(buf.get() != 0)
  }

  def bytesFixedDecoder(length: Int): Decoder[Array[Byte]] = Decoder { buf =>
    if(buf.remaining < length) NotEnough
    else {
      val bytesFixedLength = new Array[Byte](length)
      buf.get(bytesFixedLength)
      Consumed(bytesFixedLength)
    }
  }

  val bytesDecoder: Decoder[Array[Byte]] = for {
    length <- varIntDecoder
    bytes <- bytesFixedDecoder(length)
  } yield bytes

  def stringFixedDecoder(length: Int): Decoder[String] = bytesFixedDecoder(length).map(new String(_, "UTF-8"))

  val stringDecoder: Decoder[String] = bytesDecoder.map(new String(_, "UTF-8"))

  val shortDecoder: Decoder[Short] = Decoder { buf =>
    if(buf.remaining < 1) NotEnough
    else Consumed(buf.getShort())
  }

  val intDecoder: Decoder[Int] = Decoder { buf =>
    if(buf.remaining < 4) NotEnough
    else Consumed(buf.getInt())
  }

  // val longDecoder: Decoder[Long] = Decoder { buf =>
  //   if(buf.remaining < 8) NotEnough
  //   else {
  //     val s = buf.getLong()
  //     Consumed(s)
  //   }
  // }

  // val floatDecoder: Decoder[Float] = Decoder { buf =>
  //   if(buf.remaining < 4) NotEnough
  //   else {
  //     val s = buf.getFloat()
  //     Consumed(s)
  //   }
  // }

  // val doubleDecoder: Decoder[Double] = Decoder { buf =>
  //   if(buf.remaining < 8) NotEnough
  //   else {
  //     val s = buf.getDouble()
  //     Consumed(s)
  //   }
  // }
}