package scalackh.math

// private constructor to prevent auto-derivation to Int
class UInt32 private(val unsafeInt: Int) {
  def toLong(): Long = {
    var result: Long = 0l
    result += unsafeInt >> 16 & 0xffff
    result = result << 16
    result += unsafeInt & 0xffff
    result
  }

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[UInt32] && that.asInstanceOf[UInt32].unsafeInt == unsafeInt
  }

  override def toString(): String = s"UInt32(${toLong()})"
}

object UInt32 {
  val MaxValue: Long = 4294967295l
  val MinValue: Long = 0

  def unsign(int: Int): UInt32 = new UInt32(int)

  def apply(long: Long): Option[UInt32] = {
    if(long < MinValue || MaxValue < long) None
    else Some(UInt32.unsign((long & 0xffffffff).toInt))
  }
}