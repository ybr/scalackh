package scalackh.transpose

import java.util.UUID

trait DefaultNullables {
  implicit val floatNullable: Nullable[Float] = Nullable(0.0f)
  implicit val doubleNullable: Nullable[Double] = Nullable(0.0)
  implicit val byteNullable: Nullable[Byte] = Nullable(0)
  implicit val shotNullable: Nullable[Short] = Nullable(0)
  implicit val intNullable: Nullable[Int] = Nullable(0)
  implicit val longNullable: Nullable[Long] = Nullable(0)
  implicit val stringNullable: Nullable[String] = Nullable("")
  implicit val uuidNullable: Nullable[UUID] = Nullable(new UUID(0, 0))
}