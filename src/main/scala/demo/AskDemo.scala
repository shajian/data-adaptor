package demo

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Hal {
  sealed trait Command
  case class OpenThePodBayDoorsPlease(replyTo: ActorRef[Response]) extends Command
  case class Response(message: String)

  def apply(): Behaviors.Receive[Hal.Command] =
    Behaviors.receiveMessage[Command] {
      case OpenThePodBayDoorsPlease(replyTo) =>
        replyTo ! Response("I'm sorry I can't do that.")
        Behaviors.same
    }
}

object Dave {
  sealed trait Command
  private case class AdaptedResponse(message: String) extends Command

  def apply(hal: ActorRef[Hal.Command]): Behavior[Dave.Command] =
    Behaviors.setup[Command] { context =>
      implicit val timeout: Timeout = 3.seconds

      context.ask(hal, Hal.OpenThePodBayDoorsPlease) {
        case Success(Hal.Response(message)) => AdaptedResponse(message)
        case Failure(_)                     => AdaptedResponse("request failed")
      }

      val requestId = 1
      context.ask(hal, Hal.OpenThePodBayDoorsPlease) {
        case Success(Hal.Response(message)) => AdaptedResponse(s"$requestId: $message")
        case Failure(_)                     => AdaptedResponse(s"requestId: Request failed")
      }

      Behaviors.receiveMessage {
        case AdaptedResponse(message) =>
          context.log.info("Got response from hal: {}", message)
          Behaviors.same
      }
    }
}
object AskDemo extends App {
//  val hal = ActorSystem(Hal(), "hal")
//  val dave = Dave(hal)
  def foo[T](x: List[T])(implicit m: Manifest[T]) = {
    if (m <:< manifest[String])
      println("Hey, this list is full of strings")
    else
      println("Non-stringy list")
  }
  foo(List("one", "two")) // Hey, this list is full of strings
  foo(List(1, 2)) // Non-stringy list
  foo(List("one", 2)) // Non-stringy list
}
