package scalackh.protocol.codec

import java.nio.ByteBuffer

trait Encoder[T] {
  def write(t: T, b: ByteBuffer): Unit
}

object Encoder {
  def apply[T](f: (T, ByteBuffer) => Unit): Encoder[T] =  new Encoder[T] {
    def write(t: T, b: ByteBuffer): Unit = f(t, b)
  }
}