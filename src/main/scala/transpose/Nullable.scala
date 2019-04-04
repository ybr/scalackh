package scalackh.transpose

trait Nullable[A] {
  def nullValue(): A
}

object Nullable {
  def apply[A](a: A): Nullable[A] = new Nullable[A] {
    val nullValue: A = a
  }
}
