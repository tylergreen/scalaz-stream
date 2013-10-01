package scalaz.stream

import scalaz.concurrent.Task

class Scoped[S,+A](private[stream] val get: Task[A]) {
  def map[B](f: A => B): Scoped[S,B] = new Scoped(get.map(f))
  def flatMap[B](f: A => Scoped[S,B]): Scoped[S,B] = 
    new Scoped(get.flatMap(f andThen (_.get)))
}
object Scoped {
  def apply[S,A](a: => A): Scoped[S,A] = 
    new Scoped(Task.delay(a))
}
