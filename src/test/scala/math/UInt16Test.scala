package scalackh.math

import org.scalacheck._
import org.scalacheck.Prop.forAll

object UInt16Test extends Properties("UInt16") {
  property("valid in [0;65535]") = forAll(Gen.chooseNum[Int](0, UInt16.MaxValue)) { (n1: Int) =>
    UInt16(n1).get.toInt == n1
  }

  property("not valid in [-MinValue;0[") = forAll(Gen.chooseNum[Int](Int.MinValue, -1)) { (n1: Int) =>
    UInt16(n1) == None
  }

  property("not valid in ]65535;MaxValue]") = forAll(Gen.chooseNum[Int](UInt16.MaxValue + 1, Int.MaxValue)) { (n1: Int) =>
    UInt16(n1) == None
  }

  property("valid unsafe in [-32768;-1] maps to higher half of utin16") = forAll(Gen.chooseNum[Short](-32768, -1).filter(_ < 0)) { (n1: Short) =>
    val n2 = UInt16.unsign(n1).toInt
    n2 == UInt16.MaxValue + 1 + n1
  }
}