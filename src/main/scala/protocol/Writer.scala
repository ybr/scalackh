import java.nio.ByteBuffer

trait Writer[T] {
  def write(t: T, b: ByteBuffer): Unit
}

object Writer {
  def apply[T](f: (T, ByteBuffer) => Unit): Writer[T] =  new Writer[T] {
    def write(t: T, b: ByteBuffer): Unit = f(t, b)
  }
}