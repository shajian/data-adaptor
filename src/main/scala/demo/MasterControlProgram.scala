package demo

import akka.actor.typed.{ActorSystem, Behavior, PostStop, Terminated}
import akka.actor.typed.scaladsl.Behaviors
import org.jline.builtins.Builtins.Command
import org.slf4j.Logger

import scala.concurrent.Await

object MasterControlProgram {
  sealed trait Command
  final case class SpawnJob(name: String) extends Command
  case object GracefulShutdown extends Command

  def cleanup(log: Logger): Unit = log.info("Cleaning up!")

  def apply(): Behavior[Command] = {
    Behaviors
      .receive[Command] { (context, message) =>
        message match {
          case SpawnJob(jobName) =>
            context.log.info("Spawning job {}!", jobName)
            val job = context.spawn(Job(jobName), name = jobName)
            context.watch(job)
            job ! Job.JobNotice("s")
            Behaviors.same
          case GracefulShutdown =>
            context.log.info("Initiating graceful shutdown...")
            Behaviors.stopped { () =>
              Thread.sleep(2000)
              cleanup(context.system.log)
            }
        }
    }
      .receiveSignal{
        case (context, PostStop) =>
          Thread.sleep(3000)
          context.log.info("Master Control Program stopped")
          Behaviors.same
        case (context, Terminated(ref)) =>
          Thread.sleep(2000)
          context.log.info("Job stopped: {}", ref.path.name)
          Behaviors.same
      }
  }
}

object Job {
  sealed trait Command
  final case class JobNotice(msg: String) extends Command

  def apply(name: String): Behavior[Command] = {
    Behaviors
        .receiveMessage[Command] { message =>
        message match {
          case JobNotice(msg) =>
            Behaviors.stopped
        }

    }
      .receiveSignal {
        case (context, PostStop) =>
          context.log.info("Worker {} stopped", name)
          Behaviors.stopped
      }
  }
}


object MasterCtrlDemo extends App {
  import MasterControlProgram._
  import scala.concurrent.duration._
  val system: ActorSystem[MasterControlProgram.Command] = ActorSystem(MasterControlProgram(), "B7780")

  system ! SpawnJob("a")
  system ! SpawnJob("b")

  Thread.sleep(10000)
  system ! GracefulShutdown

  Thread.sleep(100)
  Await.result(system.whenTerminated, 15.seconds)
}