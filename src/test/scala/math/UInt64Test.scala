package scalackh.math

import scala.math.BigInt

import org.scalacheck._
import org.scalacheck.Prop.forAll

object UInt64Test extends Properties("UInt64") {
  val longs: Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)

  property("valid in [0;2^64-1]") = forAll(longs.map(x => BigInt(x) + BigInt(2).pow(63))) { (n1: BigInt) =>
    UInt64(n1).get.toBigInt == n1
  }

  property("not valid in [-MinValue;0[") = forAll(Gen.chooseNum(Long.MinValue, -1).map(BigInt(_))) { (n1: BigInt) =>
    UInt64(n1) == None
  }

  property("not valid in ]2^64;MaxValue]") = forAll(longs.map(x => BigInt(x).abs + BigInt(2).pow(64))) { (n1: BigInt) =>
    UInt64(n1) == None
  }

  property("valid unsafe in [-2^63;-1] maps to higher half of utin32") = forAll(Gen.chooseNum[Long](Long.MinValue, -1).filter(_ < 0)) { (n1: Long) =>
    val n2 = UInt64.unsign(n1).toBigInt
    n2 == UInt64.MaxValue + 1 + BigInt(n1)
  }
}