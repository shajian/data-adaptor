package demo

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration._
import akka.actor.{ActorSystem, CoordinatedShutdown}

import scala.concurrent.Future

final case class EscalateException(message: String) extends RuntimeException(message)
final case class ActorException(ref: ActorRef[Nothing], cause: Throwable) extends RuntimeException(cause)

object Child {
  sealed trait Command
  case object ThrowNormalException extends Command
  case object ThrowEscalateException extends Command
  def apply(): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("started.")
    Behaviors.receiveMessage[Command] {
      case ThrowEscalateException => throw EscalateException("this is escalate exception.")
      case ThrowNormalException => throw new RuntimeException("this is normal exception.")
    }
      .receiveSignal {
        case (_, PreRestart) =>
          context.log.info("Pre restart.")
          Behaviors.same
        case (_, PostStop) =>
          context.log.info("stopped.")
          Behaviors.same
      }
  }
}

object Parent {
  sealed trait Command
  def apply(): Behavior[Command] = Behaviors.setup { context =>
    import context.executionContext
    val child1 = context.spawn(Child(), "child1")
    context.watch(child1)
    val child2 = context.spawn(Child(), "child2")
    context.watch(child2)
    child2 ! Child.ThrowNormalException
    context.system.scheduler.scheduleOnce(1.second, () => child1 ! Child.ThrowEscalateException)
    Behaviors.receiveSignal {
      case (_, ChildFailed(ref, e: EscalateException)) => throw ActorException(ref, e)
      case (_, ChildFailed(ref, e)) =>
        context.log.warn(s"Received child actor ${ref.path} terminated signal, original exception is $e")
        Behaviors.same
    }
  }
}

object Root {
  sealed trait Command
  def apply(): Behavior[Command] = Behaviors.setup { context =>
    val parent = context.spawn(Parent(), "parent")
    context.watch(parent)
    Behaviors.receiveSignal {
      case (_, ChildFailed(ref, e)) =>
        context.log.info(s"Received parent actor ${ref.path} failed signal, original exception is $e")
        Behaviors.same
      case (_, Terminated(ref)) =>
        context.log.info(s"Received actor ${ref.path} terminated signal.")
        Behaviors.same
    }
  }
}

object WatchActorMain {
//  def main(args: Array[String]): Unit = {
//    val system = ActorSystem(Root(), "watch")
//    TimeUnit.SECONDS.sleep(2)
//    system.terminate()
//  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()

    CoordinatedShutdown(system)
      .addCancellableTask(CoordinatedShutdown.PhaseServiceRequestsDone, "ColoseJdbcDataSource") { () =>
        Future {
          println("Close JDBC DataSource.")
          Done
        }(system.dispatcher)
      }

    CoordinatedShutdown(system).addJvmShutdownHook {
      println("JVM shutdown hook.")
    }

    system.terminate()
  }
}
