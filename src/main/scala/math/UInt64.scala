package scalackh.math

import scala.math.BigInt

// private constructor to prevent auto-derivation to Long
class UInt64 private(val unsafeLong: Long) {
  def toBigInt(): BigInt = {
    var big: BigInt = 0
    big = big + ((unsafeLong >> 48) & 0xffff)
    big = big << 16
    big = big + ((unsafeLong >> 32) & 0xffff)
    big = big << 16
    big = big + ((unsafeLong >> 16) & 0xffff)
    big = big << 16
    big = big + (unsafeLong & 0xffff)
    big
  }

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[UInt64] && that.asInstanceOf[UInt64].unsafeLong == unsafeLong
  }

  override def toString(): String = s"UInt64(${toBigInt()})"
}

object UInt64 {
  val MaxValue: BigInt = BigInt("18446744073709551615")
  val MinValue: BigInt = BigInt(0)

  def unsign(long: Long): UInt64 = new UInt64(long)

  def apply(big: BigInt): Option[UInt64] = {
    if(big < MinValue || MaxValue < big) None
    else Some(UInt64.unsign(big.toLong))
  }
}