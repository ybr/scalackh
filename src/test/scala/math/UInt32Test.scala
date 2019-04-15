package scalackh.math

import org.scalacheck._
import org.scalacheck.Prop.forAll

object UInt32Test extends Properties("UInt32") {
  property("valid in [0;4294967295]") = forAll(Gen.chooseNum[Long](0, UInt32.MaxValue)) { (n1: Long) =>
    UInt32(n1).get.toLong == n1
  }

  property("not valid in [-MinValue;0[") = forAll(Gen.chooseNum[Long](Long.MinValue, -1)) { (n1: Long) =>
    UInt32(n1) == None
  }

  property("not valid in ]4294967295;MaxValue]") = forAll(Gen.chooseNum[Long](UInt32.MaxValue + 1, Long.MaxValue)) { (n1: Long) =>
    UInt32(n1) == None
  }

  property("valid unsafe in [-2147483648;-1] maps to higher half of utin32") = forAll(Gen.chooseNum[Int](-2147483648, -1).filter(_ < 0)) { (n1: Int) =>
    val n2 = UInt32(n1).toLong
    n2 == UInt32.MaxValue + 1 + n1
  }
}