package scalackh.math

// private constructor to prevent auto-derivation to Byte
class UInt8 private(val unsafeByte: Byte) {
  def toShort(): Short = (unsafeByte & 0xff).toShort

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[UInt8] && that.asInstanceOf[UInt8].unsafeByte == unsafeByte
  }

  override def toString(): String = s"UInt8(${toShort()})"
}

object UInt8 {
  val MaxValue: Short = 255
  val MinValue: Short = 0

  def unsign(byte: Byte): UInt8 = new UInt8(byte)

  def apply(short: Short): Option[UInt8] = {
    if(short < MinValue || MaxValue < short) None
    else Some(UInt8.unsign(short.toByte))
  }
}