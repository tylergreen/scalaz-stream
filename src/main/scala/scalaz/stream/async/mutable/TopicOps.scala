package scalaz.stream.async.mutable

import scalaz.syntax.Ops
import scalaz.concurrent.Strategy
import scalaz.stream.Process._


trait TopicOps[A] extends Ops[Topic[A]] {
  /**
   * Gets _journal_ topic, that will use supplied journal to archive published messages, so they can be later 
   * fed to subscriber for reconciliation on subscriber's subscription. 
   *
   * {{
   *
   *  //journal that keeps history of 5 minutes
   *  async.topic[A],journal(Topic.journal.last (5 minutes)) 
   *
   *  //journal that keeps history of 10 messages
   *  async.topic[A],journal(Topic.journal.lastN (10)) 
   *
   *  //journal that keeps only numbers from the last even number
   *  async.topic[Int],journal(Topic.journal.keyFrame (_ % 2 == 0))  
   *
   * }}
   *
   * @param journal  Journal process, that holds any history of publishing the `A` to this topic. History is fed to 
   *                 subscriber for reconciliation when subscriber subscribes to this topic. `A` are extracted from
   *                 journal by invoking `journal.flush`
   */
  def journal(journal:Process1[A,A])(implicit S: Strategy = Strategy.DefaultStrategy) : Topic[A] = new Topic[A] {
    private[stream] lazy val actor = scalaz.stream.actor.topic[A](journal)(S)
  }
}
