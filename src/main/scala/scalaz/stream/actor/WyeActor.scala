package scalaz.stream.actor

import scalaz.concurrent.{Strategy, Actor, Task}
import scalaz.stream.Process
import scalaz.stream.Process._
import scalaz.stream.actor.message.wye.{Get, Run, Ready, Done, Side, Msg}
import scalaz.stream.wye.{AwaitBoth, AwaitR, AwaitL}
import scala.annotation.tailrec
import scalaz._
import scalaz.stream.{Step, wye}


object debug {
  def apply(s: String, o:Any*) {
     //println(s,o)
  }
}

/**
 * Created by pach on 11/18/13.
 */
trait WyeActor {


  def wyeActor[L, R, O](p1: Process[Task, L], p2: Process[Task, R])(y: Wye[L, R, O]): Process[Task, O] = {


    //when any of this is set, this indicates that side is ready to be `read`
    // left - process has terminated
    // right - process is ready for next read, with next state
    // when unset, indicates the process is likely in await, or is performing a cleanup
    // wye (out) shall end only in case both sl and sr are set to Some(-\/(e))
    var sl: Option[Throwable \/ (Seq[L], Process[Task, L])] = None
    var sr: Option[Throwable \/ (Seq[R], Process[Task, R])] = None

    //state of wye
    var yy = y

    //output to be taken
    // - left when wye fails, contains reason and `out` on next `Get` will fail with that reason.
    //   If out terminated, this is set to reason, and callback that must be calles once left, right and wye terminate
    // - middle - values to be emitted to out
    // - right - waiting for either left right or both to be processed by wye
    var out: Either3[(Throwable, Option[(Throwable \/ Unit) => Unit]), Seq[O], (Throwable \/ Seq[O]) => Unit] = Middle3(Nil)
    //var out: (Throwable \/ Seq[O]) \/ ((Throwable \/ Seq[O]) => Unit) = -\/(\/-(Nil))

    //bias to have fair queueing in case of AwaitBoth
    var bias: Side.Value = Side.L



    //we use step now, maybe shall optimize this a bit to avoid messaging in case of p being in Halt or Emit
    def pull[A](side: Side.Value)(p: Process[Task, A], actor: Actor[Msg]): Unit = {
      debug("PULL " + side, p)
      p.step.runLast.runAsync {
        cb =>
          debug("PULLED " + side,cb)
          val next = cb.fold(
          t => -\/(t)
          , {
            case Some(step) => \/-(step)
            case None       => -\/(End)
          })
          actor ! Ready(side, next)
      }
    }

    def pullL = pull[L](Side.L) _
    def pullR = pull[R](Side.R) _


    //terminates the p1 or p2
    //this is called from wye in halt, and actually
    def kill[A](side: Side.Value, rsn: Throwable, actor: Actor[Msg]) = {
      debug("KILL",side,sl,sr)
      val s = side match {
        case Side.L => sl
        case Side.R => sr
      }

      s match {
        case Some(\/-((_,step: Process[Task, A]))) =>
          side match {
            case Side.L => sl = None
            case Side.R => sr = None
          }
          step.killBy(rsn).run.runAsync { _ =>
            debug("Killed", side, sl, sr)
            actor ! Ready(side, -\/(rsn)) }
        case _                              => //no-op hence we will run it once set (None=>Some) or is already failed
      }


    }


    //this interprets wye and is `protected` by actor
    // it keeps state of wye in `yy`
    def runWye(actor: Actor[Msg]) {
      @tailrec
      def go(y2: Wye[L, R, O]): Wye[L, R, O] = {

        def readAndPull[A](side: Side.Value)
                          (as: Option[Throwable \/ (Seq[A],Process[Task, A])], setF: Option[Throwable \/ (Seq[A],Process[Task, A])] => Unit)
                          (feed: Seq[A] => Wye[L, R, O] => Wye[L, R, O])
                          (awy: Await[Env[L, R]#Y, A, O]): Option[Wye[L, R, O]] = {
          debug("PULL+", side, sl, sr, as, out, awy)
          as match {
            case Some(-\/(End))  => debug(s"$side is End"); Some(awy.kill)
            case Some(-\/(t))    => debug(s"$side is t", t.getClass.getName);Some(awy.killBy(t))
            case Some(\/-((hd,p))) =>
              setF(None)
              pull[A](side)(p, actor)
              side match {
                case Side.L => bias = Side.R
                case Side.R => bias = Side.L
              }
              val r = Some(feed(hd)(awy))
              debug("PULL", side, r)
              r
            case None            =>
              debug("NRDY", side)
              None
          }
        }

        def readAndPullLeft(awy: Wye[L, R, O]) = readAndPull[L](Side.L)(sl, sl = _)(wye.feedL)(awy.asInstanceOf[Await[Env[L, R]#Y, L, O]])
        def readAndPullRight(awy: Wye[L, R, O]) = readAndPull[R](Side.R)(sr, sr = _)(wye.feedR)(awy.asInstanceOf[Await[Env[L, R]#Y, R, O]])


        debug("$Y$", y2, sl, sr, out)

        y2 match {

          case h@Halt(rsn) =>
            debug("#Y# HALT", rsn, sl, sr, out)
            if (sl.exists(_.isLeft) && sr.exists(_.isLeft)) {
              //both sides got killed we can `kill` the out side
              out match {
                case Right3(cb) => cb(-\/(rsn))
                case Left3((rsn,Some(cb))) => cb(\/-()) //signal cleanups are done
                case _          => //no-op
              }
              out = Left3((rsn, None))
            } else {
              debug("killing ********* ")
              kill(Side.L, rsn, actor)
              kill(Side.R, rsn, actor)
            }
            y2

          case Emit(h, nextY) =>
            debug("#Y# EMIT", sl, sr, out, y2)
            if (h.nonEmpty) {
              out match {
                //callback is waiting for out
                case Right3(cb) => out = Middle3(Nil); cb(\/-(h)); debug("OUT !", out); go(nextY)
                //some elements are waiting for out to be taken, or we add some, wait for out to take it at next GET
                case Middle3(curr) => out = Middle3(curr ++ h); debug("OUT +", h, out); nextY
                //out failed
                case Left3((err, _)) => debug("OUT =", out); go(y2.killBy(err))
              }
            } else {
              go(nextY)
            }



          case AwaitL(rcv, fb, c) =>
            debug("#Y# AwaitL", sl, sr, out)
            readAndPullLeft(y2) match {
              case Some(next) => go(next)
              case None       => y2
            }


          case AwaitR(rcv, fb, c) =>
            debug("#Y# AwaitR", sl, sr, out)
            readAndPullRight(y2) match {
              case Some(next) => go(next)
              case None       => y2
            }

          case AwaitBoth(rcv, fb, c) =>
            debug("#Y# AwaitBoth", bias, sl, sr, out)


            (sl, sr) match {
              // both are ready
              case (Some(\/-((hdL,pL))), Some(\/-((hdR,pR)))) =>
                //hence there is not feedThese, we  first feed  on bias, the other side will be fed in next cycle
                bias match {
                  case Side.L =>
                    sl = None
                    pullL(pL, actor)
                    bias = Side.R
                    go(wye.feedL(hdL)(y2))

                  case Side.R =>
                    sr = None
                    pullR(pR, actor)
                    bias = Side.L
                    go(wye.feedR(hdR)(y2))
                }

              //left is ready
              case (Some(\/-((hdL,pL))), _) =>
                sl = None
                pullL(pL, actor)
                bias = Side.R
                go(wye.feedL(hdL)(y2))

              //right is ready
              case (_, Some(\/-((hdR,pR)))) =>
                sr = None
                pullR(pR, actor)
                bias = Side.L
                go(wye.feedR(hdR)(y2))

              //both inputs are terminated, go to fallback, no more input expected
              case (Some(-\/(eL)), Some(-\/(eR))) => go(fb)

              //either in progress or one failed, but not both, just end
              case (_, _) => y2
            }

        }
      }

      //run go only has nothing emitted, otherwise wait for out`s Get
      if (out.fold(_=>true,o=>o.isEmpty,_=>true)) {
        yy = go(yy)
        debug("@Y@", yy)
      }
    }

    // seems like we can`t get inside actor handle for actor itself so this is nasty hack for now
    var actor: Actor[Msg] = null


    def ready[A](side:Side.Value,  setf: (Option[Throwable \/ (Seq[A], Process[Task, A])]) => Unit)(next:Step[Task,A]) = {
      next.head.fold(
        t =>
          next.tail match {
            case Halt(_) =>
              debug("ERRH", side,t )
              setf(Some(-\/(t))); runWye(actor) //fail it and run wye
            case o       =>
              debug("ERR", side, t, o)
              pull(side)(next.cleanup, actor) //run cleanup, no need to run wye, nothing changed. if we would run wye now, we may indicate to it too early that process was cleaned, and as such may end up w/o cleanup
          }
        , v => {
          debug("READY " + side + "1", v)
          setf(Some(\/-(v, next.tail))) //todo: don`t we have to add cleanup to tail here?
          debug("READY " + side + "2", sl,sr)
          runWye(actor)
        }
      )
    }

    def readyL(next:Step[Task,L]) =  ready[L](Side.L, sl = _)(next)
    def readyR(next:Step[Task,R]) = ready[R](Side.R, sr = _)(next)

    // Actor that does the `main` job
    val a: Actor[Msg] = Actor.actor[Msg]({

      // this is just to `start` the wye once `out` is run
      case Run =>
        pullL(p1,actor)
        pullR(p2,actor)
        runWye(actor)

      //Left side signalling failure to run Step
      case Ready(Side.L, -\/(t)) =>
        debug("ERRR L", t)
        sl = Some(-\/(t))
        runWye(actor)

      //Right side signalling failure to run Step
      case Ready(Side.R, -\/(t)) =>
        debug("ERRR R", t)
        sr = Some(-\/(t))
        runWye(actor)

      //Left side is having one step Done
      case Ready(Side.L, \/-(next: Step[Task, L]@unchecked)) =>
        debug("READY L", sl, sr, out)
        readyL(next)

      //Right side is having one step done
      case Ready(Side.R, \/-(next: Step[Task, R]@unchecked)) =>
        debug("READY R", sl, sr, out)
        readyR(next)

      //Out is trying to get next value after merge
      case Get(cb: ((Throwable \/ Seq[O]) => Unit)@unchecked) =>
        out match {
          case Left3((t,_))     => cb(-\/(t))
          case Middle3(Nil) => out = Right3(cb)
          case Middle3(hd)  => out = Middle3(Nil); cb(\/-(hd))
          case Right3(_)    => cb(-\/(new Exception("Only one callback for wye-out")))
        }
        debug("GET",sl,sr,out,yy)
        runWye(actor)

      //Out has finished, run cleanup on Left and Right and then complete
      case Done(cb) =>
        out = Left3((End,Some(cb))) //by default this is an end. We probably would rather get the exception and propagate it here
        yy == yy.killBy(End)
        debug("DONE", yy, sl,sr,out)
        runWye(actor)


    })(Strategy.DefaultStrategy)

    actor = a

    eval(Task.delay(actor ! Run)).drain ++
      repeatEval(Task.async[Seq[O]](cb => actor ! Get(cb)).map { v=> debug("GETF",v);v}).flatMap(emitAll).map{v => debug("OUTP",v); v} onComplete
    eval(Task.async[Unit](cb=> actor! Done(cb))).drain


  }


}