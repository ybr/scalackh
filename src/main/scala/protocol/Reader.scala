import java.nio.ByteBuffer

trait Reader[T] {
  def read(b: ByteBuffer): T
}

object Reader {
  def apply[T](f: ByteBuffer => T): Reader[T] =  new Reader[T] {
    def read(b: ByteBuffer): T = f(b)
  }
}