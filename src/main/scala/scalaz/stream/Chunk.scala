package scalaz.stream

import Process._
import scalaz.concurrent._
import java.nio._

sealed trait Chunk[S, @specialized A] {
  def foldN[B](n: Long)(z: B)(f: (B,A) => B): Scoped[S,B]
  def feed[B](n: Long)(p: Process1[A,B]): Scoped[S,Process1[A,B]] =
    foldN(n)(p)((p,a) => process1.feed1(a)(p))
}

object Chunk {
  trait Fold[A,B] {
    def run[S](c: Chunk[S,A]): Scoped[S,B]
  }
  class Bytes[S](arr: Array[Byte], var pos: Int) extends Chunk[S,Byte] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Byte) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && pos < arr.length) {
        b = f(b,arr(pos)) 
        i += 1
        pos += 1
      }
      b
    }
    def copyN(n: Int): Scoped[S, Array[Byte]] = Scoped {
      val m = n min (arr.length - pos) 
      val res = new Array[Byte](m)
      Array.copy(arr, pos, res, 0, m)
      res
    }
  }
  class NIOBytes[S](buf: ByteBuffer) extends Chunk[S,Byte] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Byte) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
    def shorts: NIOShorts[S] = new NIOShorts(buf.asShortBuffer)
    def chars: NIOChars[S] = new NIOChars(buf.asCharBuffer)
    def floats: NIOFloats[S] = new NIOFloats(buf.asFloatBuffer)
    def doubles: NIODoubles[S] = new NIODoubles(buf.asDoubleBuffer)
    def longs: NIOLongs[S] = new NIOLongs(buf.asLongBuffer)
  }

  class NIOChars[S](buf: CharBuffer) extends Chunk[S,Char] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Char) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }

  class NIODoubles[S](buf: DoubleBuffer) extends Chunk[S,Double] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Double) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }

  class NIOInts[S](buf: IntBuffer) extends Chunk[S,Int] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Int) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }

  class NIOLongs[S](buf: LongBuffer) extends Chunk[S,Long] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Long) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }
  class NIOShorts[S](buf: ShortBuffer) extends Chunk[S,Short] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Short) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }
  class NIOFloats[S](buf: FloatBuffer) extends Chunk[S,Float] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Float) => B): Scoped[S,B] = Scoped {
      var b = z
      var i = 0L
      while (i < n && buf.hasRemaining()) {
        b = f(b,buf.get()) 
        i += 1
      }
      b
    }
  }

  object Bytes {
    def unapply[S,A](c: Chunk[S,A]): Option[Bytes[S]] =
      if (c.getClass == classOf[Bytes[S]]) Some(c.asInstanceOf[Bytes[S]])
      else None
  }
  object NIOBytes {
    def unapply[S,A](c: Chunk[S,A]): Option[NIOBytes[S]] =
      if (c.getClass == classOf[NIOBytes[S]]) Some(c.asInstanceOf[NIOBytes[S]])
      else None
  }
}
