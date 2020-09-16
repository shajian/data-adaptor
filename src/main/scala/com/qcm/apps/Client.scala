package com.qcm.apps

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Client {
  sealed class Command()
  case class Start(num: Int) extends Command
  case class Request(name: String, replyTo: ActorRef[Command]) extends Command
  case class Response(name: String, msg: String) extends Command
  case class Fail(name: String, msg: String) extends Command
  case class Stop() extends Command

  object TaskCaller {


    def apply(): Behavior[Command] = Behaviors.setup[Command] { context =>
      val proxies = (1 to 100).map(i => (i.toString, context.spawn(Proxy(), i.toString)))

      Behaviors.receiveMessage {
        case Start(num) =>
          // do `num` requesting concurrently
          proxies.take(num).foreach(p => p._2 ! Request(p._1, context.self))
//          Thread.sleep(1000)
//          // and then do requesting little batch by little batch, with some interval in each two batch
//          for (i <- 0 until 64) {
//            val proxy = proxies(i)
//            proxy._2 ! Request(proxy._1, context.self)
//            if ((i+1) % 8 == 0) Thread.sleep(1000)
//          }
          Behaviors.same
        case Response(name, msg) =>
          val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
//          context.log.info(s"get response '$msg' from $name at time $now")
          println(s"get response '$msg' from $name at time $now")
          Behaviors.same
        case Fail(name, msg) =>
          val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
//          context.log.info(s"failed for $name at time $now, error: $msg")
          println(s"failed for $name at time $now, error: $msg")
          Behaviors.same
        case Stop() =>
          Behaviors.stopped { () =>
            Thread.sleep(2000)
            context.system.log.info("Tasks is stopping")
          }
      }
    }
  }

  implicit val system = ActorSystem(TaskCaller(), "WebClient")
  implicit val ec = system.executionContext

  object Proxy {
//    import Client._

//    val pool = Http().cachedHostConnectionPool[Promise[HttpResponse]]("localhost", 8181,
//      ConnectionPoolSettings(ConfigFactory.load()))
//    val queue = Source.queue[(HttpRequest, Promise[HttpResponse])](16, OverflowStrategy.dropNew)
//      .via(pool)
//      .toMat(Sink.foreach({
//        case ((Success(resp), p)) => p.success(resp)
//        case ((Failure(e), p)) => p.failure(e)
//      }))(Keep.left)
//      .run
//    val promise = Promise[HttpResponse]
    val connectionFlow = Http().outgoingConnection(host = "localhost", port = 8181)

    def apply(): Behavior[Command] = {
      Behaviors
        .receiveMessage[Command] {
          case Request(name, replyTo) =>
            // change to use connection pool, otherwise at most 32 requests can be handled at once
//            val resp = Http().singleRequest(HttpRequest(uri = "http://localhost:8181/start/" + name))
//            resp.onComplete {
//              case Success(r) => Unmarshal(r.entity).to[String].onComplete {
//                case Success(body) => replyTo ! Response(name, body)
//                case _ => replyTo ! Fail(name, "failed to get response body")
//              }
//              case Failure(_) => replyTo ! Fail(name, "failed to get response")
//            }
//            val resp = queue.offer(HttpRequest(uri = "/start/"+name) -> promise).flatMap {
//              case QueueOfferResult.Enqueued    => promise.future
//              case QueueOfferResult.Dropped     => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
//              case QueueOfferResult.Failure(ex) => Future.failed(ex)
//              case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
//            }
            val resp = Source.single(HttpRequest(uri = "/start/"+name)).via(connectionFlow).runWith(Sink.head)
            resp.onComplete {
              case Success(r) =>
//                val header = r.headers(0)
//                println(s"header: $header")
                Unmarshal(r.entity).to[String].onComplete {
                  case Success(body) => replyTo ! Response(name, body)
                  case Failure(exception) => replyTo ! Fail(name, exception.getMessage)
                }
              case Failure(exception) => replyTo ! Fail(name, exception.getMessage)
            }
            Behaviors.stopped
        }
//        .receiveSignal {
//          case (context, PostStop) =>
//            context.log.info("proxy stopped")
//            Behaviors.stopped
//        }
    }
  }


  def main(args: Array[String]): Unit = {
    val num = if (args.size > 0) args(0).toInt else 50
    println(num)
    system ! Start(num)

    Thread.sleep(5000)
    system ! Stop()
  }
}
