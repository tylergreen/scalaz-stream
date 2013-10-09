package scalaz.stream.async.mutable

import scalaz.stream.message
import scalaz.concurrent.{Strategy, Task, Actor}
import scalaz.stream.Process
import scalaz.stream.Process._
import scalaz.stream.processes._
import scala.concurrent.duration.{FiniteDuration, Deadline, Duration}
import scalaz.syntax.Ops

/**
 * Represents topic, that asynchronously exchanges messages between one or more publisher(s) 
 * and one or more subscriber(s). 
 *
 * Guarantees:
 * - Order of messages from publisher is guaranteed to be preserved to all subscribers
 * - Messages from publishers may interleave in non deterministic order before they are read by subscribers
 * - Once the `subscriber` is run it will receive all messages from all `publishers` starting with very first message
 * arrived AFTER the subscriber was run
 *
 * Please not that topic is `active` even when there are no publishers or subscribers attached to it. However
 * once the `close` or `fail` is called all the publishers and subscribers will terminate or fail.
 *
 */
trait Topic[A] {

  import message.topic._

  private[stream] val actor: Actor[Msg[A]]


  /**
   * Gets publisher to this topic. There may be multiple publishers to this topic.
   */
  val publish: Sink[Task, A] = repeatEval(Task.now(publishOne _))

  /**
   * Gets subscriber from this topic. There may be multiple subscribers to this topic. Subscriber
   * subscribes and un-subscribes when it is run or terminated.   
   */
  val subscribe: Process[Task, A] = subscribe(id,Topic.buffer.all)

  /**
   * Will get subscriber, that before will get any of `A` messages from this topic, it will 
   * try to reconcile the `journaled` messages published in this topic to some state of `A`.
   * 
   * {{{
   *    
   *   
   *   import scalaz.stream.processes._
   *   
   *   //this does not do any reconciliation and enqueue all messages when not ready to process them
   *   subscribe(id, Topic.buffer.all)
   *   
   *   //this only emits last element during reconcile process and would not buffer any messages
   *   subscribe(last,Topic.buffer.none)
   *   
   *   
   * }}}
   * 
   *
   * @param reconcile    Process that may filter, collect, or differently alter journaled messages 
   * @param buffer       Process to buffer all `A` fed to this subscriber from this topic, when the subscriber 
   *                     is not ready to  accept more `A`. Process is consulted only after reconciliation is done, and 
   *                     when there are messages that needs to be buffered.  
   */
  def subscribe(reconcile: Process1[A, A], buffer: Process1[A, A]): Process[Task, A] = {
    await(Task.async[(Seq[A],SubscriberRef[A])](reg => actor ! Subscribe(reg, reconcile, buffer)))({
      case (reconciled,sref) => 
        emitAll(reconciled) ++ 
        repeatEval(Task.async[Seq[A]] { reg => actor ! Get(sref, reg) })
          .flatMap(l => emitAll(l))
          .onComplete(eval(Task.async[Unit](reg => actor ! UnSubscribe(sref, reg))).drain)
      }
      , halt
      , halt)
  }

  /**
   * publishes single `A` to this topic. 
   */
  def publishOne(a: A): Task[Unit] = Task.async[Unit](reg => actor ! Publish(a, reg))

  /**
   * Will `finish` this topic. Once `finished` all publishers and subscribers are halted via `halt`.
   * When this topic is `finished` or `failed` this is no-op 
   */
  def close: Task[Unit] = fail(End)

  /**
   * Will `fail` this topic. Once `failed` all publishers and subscribers will terminate with cause `err`.
   * When this topic is `finished` or `failed` this is no-op 
   */
  def fail(err: Throwable): Task[Unit] = Task.async[Unit](reg => actor ! Fail(err, reg)).attempt.map(_ => ())


}



object Topic {
  
  
  implicit class TopicSyntax[A](val self:Topic[A]) extends TopicOps[A] 
  

  object journal {

    /** Journal that never keeps any of `A` **/
    def none[A]: Process1[A, A] = await1[A].flatMap(_ => halt)

    /** Journal, that only keep last messages that are newer than given time **/
    def last[A](d: FiniteDuration): Process1[A, A] = {
      def go(v:Vector[(Deadline,A)]) : Process1[A,A] = 
        await1[A].flatMap { a =>
          go(v.dropWhile(_._1.isOverdue()) :+ (d.fromNow, a))
        } orElse emitAll(v.map(_._2))
      go(Vector())
    }

    /** Journal, that keeps only last `n` of elements */
    def lastN[A](n:Int): Process1[A, A] = {
      def go(v:Vector[A]) : Process1[A,A] =
        await1[A].flatMap { a =>
          if (v.size == n) {
            go(v.tail :+ a)
          } else {
            go (v :+ a)
          } 
        } orElse emitAll(v)
      go(Vector())
    }

    /** Journal, that keeps only the elements from keyframe for which `f` holds true **/
    def keyFrame[A](f: A => Boolean): Process1[A, A] = {
      def go(v:Vector[A]) : Process1[A,A] =
        await1[A].flatMap { a =>
          if (f(a)) {
            go(Vector(a))
          } else {
            go(v :+ a)
          } 
        } orElse emitAll(v)
      go(Vector())
    }
  }

  object buffer {

    /** Buffers all elements as they are published in topic **/
    def all[A]: Process1[A, A] = {
      def go(v: Vector[A]): Process1[A, A] =
        await1[A].flatMap(a => go(v :+ a)) orElse (emitAll(v))
      go(Vector())
    }

  }
  



}
  
