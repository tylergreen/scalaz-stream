package scalaz.stream

import Process._
import scalaz.concurrent._
import scalaz.effect.ST

sealed trait Chunk[S, @specialized A] {
  def size: ST[S,Int]   
  def get(i: Int): ST[S,A]
  def fold[B](z: B)(f: (B,A) => B): ST[S,B]
  def feed1[B](p: Process1[A,B]): ST[S,Process1[A,B]] =
    fold(p)((p,a) => process1.feed1(a)(p))
}

object Chunk {
  trait Reduce[A,B] {
    def run[S](c: Chunk[S,A]): ST[S,B]
  }
  class ByteArray[S](arr: Array[Byte]) extends Chunk[S,Byte] {
    def size = ST(arr.length) 
    def get(i: Int) = ST(arr(i))
    def fold[@specialized B](z: B)(f: (B,Byte) => B): ST[S,B] = ST {
      var b = z
      var i = 0
      while (i < arr.length) {
        b = f(b,arr(i)) 
        i += 1
      }
      b
    }
  }
}
