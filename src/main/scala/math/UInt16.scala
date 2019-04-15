package scalackh.math

// private constructor to prevent auto-derivation to Short
class UInt16 private(val unsafeShort: Short) {
  def toInt(): Int = UInt16.unsign(unsafeShort)

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[UInt16] && that.asInstanceOf[UInt16].unsafeShort == unsafeShort
  }

  override def toString(): String = s"UInt16(${toInt()})"
}

object UInt16 {
  val MaxValue: Int = 65535
  val MinValue: Int = 0

  def unsign(short: Short): Int = short & 0xffff

  def apply(short: Short): UInt16 = new UInt16(short)

  def apply(int: Int): Option[UInt16] = {
    if(int < MinValue || MaxValue < int) None
    else Some(UInt16(int.toShort))
  }
}