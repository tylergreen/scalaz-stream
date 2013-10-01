package scalaz.stream

import Process._
import scalaz.concurrent._
import scalaz.effect.ST
import java.nio._

sealed trait Chunk[S, @specialized A] {
  def foldN[B](n: Long)(z: B)(f: (B,A) => B): ST[S,B]
  def feed[B](n: Long)(p: Process1[A,B]): ST[S,Process1[A,B]] =
    foldN(n)(p)((p,a) => process1.feed1(a)(p))
}

object Chunk {
  trait Fold[A,B] {
    def run[S](c: Chunk[S,A]): ST[S,B]
  }
  class Bytes[S](arr: Array[Byte]) extends Chunk[S,Byte] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Byte) => B): ST[S,B] = ST {
      var b = z
      var i = 0
      while (i < n && i < arr.length) {
        b = f(b,arr(i)) 
        i += 1
      }
      b
    }
  }
  class NIOBytes[S](buf: ByteBuffer) extends Chunk[S,Byte] {
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Byte) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Char) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Double) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Int) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Long) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Short) => B): ST[S,B] = ST {
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
    def foldN[@specialized B](n: Long)(z: B)(f: (B,Float) => B): ST[S,B] = ST {
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
