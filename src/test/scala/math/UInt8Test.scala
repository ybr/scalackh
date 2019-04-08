package scalackh.math

import org.scalacheck._
import org.scalacheck.Prop.forAll

object UInt8Test extends Properties("UInt8") {
  property("valid in [0;2^8-1]") = forAll(Gen.chooseNum[Short](0, UInt8.MaxValue)) { (n1: Short) =>
    UInt8(n1).get.toShort == n1
  }

  property("not valid in [-MinValue;0[") = forAll(Gen.chooseNum[Short](Short.MinValue, -1)) { (n1: Short) =>
    UInt8(n1) == None
  }

  property("not valid in ]2^8;MaxValue]") = forAll(Gen.chooseNum[Short]((UInt8.MaxValue + 1).toShort, Short.MaxValue)) { (n1: Short) =>
    UInt8(n1) == None
  }

  property("valid unsafe in [-127;-1] maps to higher half of utin8") = forAll(Gen.chooseNum[Byte](-127, -1).filter(_ < 0)) { (n1: Byte) =>
    val n2 = UInt8.unsign(n1).toShort
    n2 == UInt8.MaxValue + 1 + n1
  }
}