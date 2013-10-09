package scalaz.stream

import org.scalacheck.{Prop, Properties}
import org.scalacheck.Prop._
import scalaz.{\/, -\/}
import scala.concurrent.SyncVar
import scalaz.concurrent.Task
import java.lang.Exception 
import scalaz.stream.Process._
import scalaz.stream.processes._ 
import scala.collection.mutable
import scalaz.stream.async.mutable.Topic
import scala.concurrent.duration._

object AsyncTopicSpec extends Properties("topic") {

  case object TestedEx extends Exception("expected in test") {
    override def fillInStackTrace = this
  }
 
  //tests basic publisher and subscriber functionality 
  //have two publishers and four subscribers. 
  //each publishers emits own half of elements (odd and evens) and all subscribers must 
  //get that messages
  property("basic") = forAll {
    l: List[Int] =>

      case class SubscriberData(
                                 endSyncVar: SyncVar[Throwable \/ Unit] = new SyncVar[Throwable \/ Unit],
                                 data: mutable.Buffer[Int] = mutable.Buffer.empty)

      val (even, odd) = (l.filter(_ % 2 == 0), l.filter(_ % 2 != 0))

      val topic = async.topic[Int]
      val subscribers = List.fill(4)(SubscriberData())

      def sink[A](f: A => Unit): Process.Sink[Task, A] = {
        io.resource[Unit, A => Task[Unit]](Task.now(()))(_ => Task.now(()))(
          _ => Task.now {
            (i: A) =>
              Task.now {
                f(i)
              }
          }
        )
      }

      def collectToBuffer(buffer: mutable.Buffer[Int]): Sink[Task, Int] = {
        sink {
          (i: Int) =>
            buffer += i
        }
      }

      subscribers.foreach {
        subs =>
          Task(topic.subscribe.to(collectToBuffer(subs.data)).run.runAsync(subs.endSyncVar.put)).run
      }

      val pubOdd = new SyncVar[Throwable \/ Unit]
      Task((Process.emitAll(odd).evalMap(Task.now(_)) to topic.publish).run.runAsync(pubOdd.put)).run

      val pubEven = new SyncVar[Throwable \/ Unit]
      Task((Process.emitAll(even).evalMap(Task.now(_)) to topic.publish).run.runAsync(pubEven.put)).run

      val oddResult = pubOdd.get(3000)
      val evenResult = pubEven.get(3000)

      topic.close.run

      def verifySubscribers(result1: SubscriberData, subId: String) = {
        val result = result1.endSyncVar.get(3000)

        (result.nonEmpty && result.get.isRight :| s"Subscriber $subId finished") &&
          ((result1.data.size == l.size) :| s"Subscriber $subId got all numbers ${result1.data} == ${l.size}") &&
          ((result1.data.filter(_ % 2 == 0) == even) :| s"Subscriber $subId got all even numbers") &&
          ((result1.data.filter(_ % 2 != 0) == odd) :| s"Subscriber $subId got all odd numbers")
      }

      (oddResult.nonEmpty && oddResult.get.isRight :| "Odd numbers were published") &&
        (evenResult.nonEmpty && evenResult.get.isRight :| "Even numbers were published") &&
        (subscribers.zipWithIndex.foldLeft(Prop.propBoolean(true)) {
          case (b, (a, index)) => b && verifySubscribers(a, index.toString)
        })

  }





  //tests once failed all the publishes, subscribers and signals will fail too
  property("fail") = forAll {
    l: List[Int] =>
      (l.size > 0 && l.size < 10000) ==> {
        val topic = async.topic[Int]
        topic.fail(TestedEx).run


        val emitted = new SyncVar[Throwable \/ Unit]
        Task((Process.emitAll(l).evalMap(Task.now(_)) to topic.publish).run.runAsync(emitted.put)).run

        val sub1 = new SyncVar[Throwable \/ Seq[Int]]

        Task(topic.subscribe.runLog.runAsync(sub1.put)).run


        emitted.get(3000)
        sub1.get(3000)


        (emitted.get(0).nonEmpty && emitted.get == -\/(TestedEx)) :| "publisher fails" &&
          (sub1.get(0).nonEmpty && sub1.get == -\/(TestedEx)) :| "subscriber fails"
      }
  } 
  
  
  //tests that subscriber will see all journal messages once subscriber to topic
  property("journal") = forAll {
    l: List[Int] =>
      (l.size > 0 && l.size < 10000) ==> {
        val topic = async.topic[Int].journal(Topic.journal.lastN(l.size))
        
        val (before,after) = l.splitAt(l.size/2)
        
        (emitAll(before).evalMap(Task.now(_)) to topic.publish).run.run

        val sub1 = new SyncVar[Throwable \/ Seq[Int]]
        Task(topic.subscribe.runLog.runAsync(sub1.put)).run
        

        val emitted = new SyncVar[Throwable \/ Unit]
        Task((Process.emitAll(after).evalMap(Task.now(_)) to topic.publish).run.runAsync(emitted.put)).run

        val sub2 = new SyncVar[Throwable \/ Seq[Int]]
        Task(topic.subscribe.runLog.runAsync(sub2.put)).run

        val emittedAll = emitted.get(3000)
        
        topic.close.run
        
        val sub1items = sub1.get(3000)
        val sub2items = sub2.get(3000)
 
        (emittedAll.nonEmpty && emittedAll.get.isRight) :| "publisher is done" &&
          (sub1items.nonEmpty && sub1items.get.isRight) :| "subscriber1 is done" &&
          (sub1items.get.toOption.getOrElse(Nil) == l)  :| "subscriber1 got all journal elements" &&
          (sub2items.nonEmpty && sub2items.get.isRight) :| "subscriber2 is done" &&
          (sub2items.get.toOption.getOrElse(Nil) == l)  :| "subscriber2 got all journal elements"
      }
    
    
  }
  


  //tests once failed all the publishes, subscribers and signals will fail too
  property("journals") = forAll {
    l: List[Int] =>
      (l.size > 0 && l.size < 10000) ==> { 
        val expectedKf = l.foldLeft(Seq[Int]()){
          case (acc,next) if next %2 == 0 => List(next)
          case (acc,next) => acc :+ next 
        }
       
        ((feed(l)(Topic.journal.none)).flush == Nil) :| "no.journal" &&
        ((feed(l)(Topic.journal.lastN(10)).flush == l.takeRight(10))) :| "lastN.journal" &&
        ((feed(l)(Topic.journal.keyFrame(_ % 2 == 0)).flush == expectedKf)) :| "keyFrame.journal"
          
      }
  }

 

  property("journals.timed") = forAll {

    l: List[Int] => 
      (l.size > 0 && l.size < 10000) ==> {
        val (before,after) = l.splitAt(l.size/2)
        val first = feed(before)(Topic.journal.last(10 millis))
        
        Thread.sleep(20)
        val second =  feed(after)(first)
        
        (first.flush == before) :| "first before eviction were collected" &&
        (second.flush == after) :| "after time were evicted"
      }

  }



  

  //
  //tests that subscriber will see only reconciled messages once get subscriber
  property("reconcile") = forAll {
    l: List[Int] =>
      (l.size > 0 && l.size < 10000) ==> {
        val topic = async.topic[Int].journal(Topic.journal.lastN(l.size))

        val (before,after) = l.splitAt(l.size/2)

        (emitAll(before).evalMap(Task.now(_)) to topic.publish).run.run

        val sub1 = new SyncVar[Throwable \/ Seq[Int]]
        topic.subscribe(filter[Int](_ % 2 == 0), Topic.buffer.all).runLog.runAsync(sub1.put)

        val emitted = new SyncVar[Throwable \/ Unit]
        Task((Process.emitAll(after).evalMap(Task.now(_)) to topic.publish).run.runAsync(emitted.put)).run
        
        val emittedAll = emitted.get(3000)

        topic.close.run

        val sub1items = sub1.get(3000) 

        (emittedAll.nonEmpty && emittedAll.get.isRight) :| "publisher is done" &&
          (sub1items.nonEmpty && sub1items.get.isRight) :| "subscriber is done" &&
          (sub1items.get.toOption.getOrElse(Nil) == before.filter(_ % 2 == 0) ++ after)  :| "subscriber1 got all journal elements, reconciled" 
      }


  } 
   
  
  // tests that subscriber actually uses buffer for all messages that subscriber was not ready to process
  property("buffer") = forAll {
    l: List[Int] => 
    (l.size > 0 && l.size < 10000) ==> { 
      val topic = async.topic[Int]

      var observer = Seq[Int]()
      
      val observedBuffer = id[Int].flatMap(i => { observer = observer :+ i ; halt })
      
      val sub1 = new SyncVar[Throwable \/ Seq[Int]] 
      Task((topic.subscribe(id, observedBuffer) zip (awakeEvery(4 millis))).map(_._1).runLog.runAsync(sub1.put)).run

      val emitted = new SyncVar[Throwable \/ Unit]
      Task((Process.emitAll(l).evalMap(v=>Task.delay{ Thread.sleep(2);v }) to topic.publish).run.runAsync(emitted.put)).run

      val emittedAll = emitted.get(3000)

      topic.close.run

      val sub1items = sub1.get(3000) 

      (emittedAll.nonEmpty && emittedAll.get.isRight) :| "publisher is done" &&
        (sub1items.nonEmpty && sub1items.get.isRight) :| "subscriber is done" &&
        (sub1items.get.toOption.getOrElse(Nil).size + observer.size == l.size)  :| "buffer or subscriber catched all" 
    }


  }     

}
