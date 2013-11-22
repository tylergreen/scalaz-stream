package scalaz.stream
package actor

import Process._
import scala.collection.immutable.IntMap
import scalaz.\/
import scalaz.\/._
import scalaz.concurrent.{Actor,Strategy,Task}
import wye.{AwaitL,AwaitR,AwaitBoth}

trait MergeN {

  def mergeN[A,B](streams: Process[Task, Process[Task,A]])(
                  y: Wye[Int,A,B]): Process[Task,B] = {
    var id = 0; def fresh = { id += 1; id }
    var outstanding = 0; def kill = { outstanding -= 1; outstanding <= 0 }
    var idle = IntMap[Process[Task,A]]()
    var rem: Option[Process[Task,Process[Task,A]]] = Some(streams)
    var cur = y
    val q = async.localQueue[Seq[B]]._1

    trait M
    case class Get(cb: Throwable \/ Seq[B] => Unit) extends M
    case class Recv(ind: Int, step: Step[Task,A]) extends M
    case class Open(stream: Step[Task, Process[Task,A]]) extends M

    var a: Actor[M] = null
    def pullL(required: Boolean) = {
      rem.foreach { _.step.runLastOr(sys.error("unpossible"))
                     .map { a ! Open(_) }
                     .runAsync(_ => ()) }
      if (required) rem.foreach(s => if (s.isHalt) q.close)
      rem = None
    }
    def pullR(required: Boolean) = {
      if (outstanding <= 0 && required) q.close
      idle.foreach { case (ind, p) => p.step.runLastOr(sys.error("unpossible"))
          .map { a ! Recv(ind, _) }
          .runAsync(_ => ())
      }
      idle = IntMap()
    }

    // not quite right - need to track requireL and requireR as state, so that
    // when messages arrive, we know whether to halt or not
    a = Actor.actor[M] {
      case Get(cb) =>
        cur match {
          case Halt(e) => q.fail(e)
          case Emit(h, t) => cur = t; cb(right(h))
          case AwaitL(_,_,_) => pullL(true)
          case AwaitR(_,_,_) => pullR(true)
          case AwaitBoth(_,_,_) =>
            if (math.random < .5) { pullL(false); pullR(false) }
            else { pullR(false); pullL(false) }
        }
        q.dequeue(cb)
      case Recv(ind, step) =>
        idle = if (step.tail.isHalt) idle else idle + (ind -> step.tail)
        step.handle { hd => cur = scalaz.stream.wye.feedR(hd)(cur) } (
          if (kill && rem.map(_.isHalt).getOrElse(false)) q.close else (),
          q.fail
        )
        val (h, t) = cur.unemit
        if (h.nonEmpty) q.enqueue(h)
        cur = t
      case Open(step) =>
        step.handle { hd =>
          idle = idle ++ hd.map((fresh,_))
          outstanding += hd.size
          cur = scalaz.stream.wye.feed1L(outstanding)(cur)
          rem = Some(step.tail)
        } (rem = Some(halt), q.fail)
    } (Strategy.Sequential)

    repeatEval(Task.async[Seq[B]](cb => a ! Get(cb))).flatMap(emitSeq(_))
  }

  def merge[A](p: Process[Task,A]*): Process[Task,A] = {
    var idle = IntMap() ++ p.zipWithIndex.map(_.swap)
    var rem = p.size; def kill: Boolean = { rem -= 1; rem <= 0 }
    val q = async.localQueue[Seq[A]]._1

    trait M
    case class Get(cb: Throwable \/ Seq[A] => Unit) extends M
    case class Recv(ind: Int, step: Step[Task,A]) extends M
    var a: Actor[M] = null
    a = Actor.actor[M] {
      case Get(cb) =>
        idle.foreach { case (ind, p) =>
          p.step.runLastOr(sys.error("unpossible"))
           .map { a ! Recv(ind, _) }
           .runAsync(_ => ())
        }
        idle = IntMap()
        q.dequeue(cb)
      case Recv(ind, step) =>
        idle = if (step.tail.isHalt) idle else idle + (ind -> step.tail)
        step.handle (q.enqueue) (if (kill) q.close else (), q.fail)
    } (Strategy.Sequential)

    repeatEval(Task.async[Seq[A]](cb => a ! Get(cb))).flatMap(emitSeq(_))
  }
}
