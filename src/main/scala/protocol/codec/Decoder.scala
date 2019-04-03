package scalackh.protocol.codec

import java.nio.ByteBuffer

trait Decoder[+A] { self =>
  // be carefull to reset byte buffer position to its initial value when returning not enough
  def read(b: ByteBuffer): DecoderResult[A]

  def flatMap[B](f: A => Decoder[B]): Decoder[B] = Decoder { buf =>
    self.read(buf) match {
      case Consumed(a) => f(a).read(buf)
      case NotEnough => NotEnough
    }
  }

  def map[B](f: A => B): Decoder[B] = Decoder { buf =>
    self.read(buf) match {
      case Consumed(a) => Consumed(f(a))
      case NotEnough => NotEnough
    }
  }
}

object Decoder {
  def apply[T](f: ByteBuffer => DecoderResult[T]): Decoder[T] = new Decoder[T] {
    def read(buf: ByteBuffer): DecoderResult[T] = {
      val position = buf.position
      f(buf) match {
        case NotEnough =>
          buf.position(position)
          NotEnough
        case consumed => consumed
      }
    }
  }

  def pure[T](value: T): Decoder[T] = new Decoder[T] {
    def read(buf: ByteBuffer): DecoderResult[T] = Consumed(value)
  }

  // turn it tailrec
  def traverse[T](rs: List[Decoder[T]]): Decoder[List[T]] = new Decoder[List[T]] {
    def read(buf: ByteBuffer): DecoderResult[List[T]] = {
      rs match {
        case head :: tail =>
          head.read(buf) match {
            case Consumed(t) => traverse(tail).read(buf) match {
              case NotEnough => NotEnough
              case Consumed(ts) => Consumed(t +: ts)
            }
            case NotEnough => NotEnough
          }

        case Nil => Consumed(List.empty)
      }
    }
  }
}

sealed trait DecoderResult[+T]
case class Consumed[T](value: T) extends DecoderResult[T]
case object NotEnough extends DecoderResult[Nothing]