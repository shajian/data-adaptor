package com.qcm.apps



import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn
import scala.concurrent.duration._
import scala.collection.mutable
import com.qcm.actors._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success




object Server {
  sealed trait Message
  case class Reply(msg: String) extends Message

  implicit val system = ActorSystem(TaskManager(), "web-task-controller")
  implicit val executionContext = system.executionContext

  def main(args: Array[String]): Unit = {

    val tasks: ActorRef[TaskManager.Command] = system

    implicit val respFormat = jsonFormat1(Reply)
    implicit val timeout: Timeout = 5.seconds

    val route =
      concat(
        pathPrefix("start" / Remaining) { name =>
          val reply: Future[Reply] = tasks.ask(ref => TaskManager.Start(name, ref))
          onSuccess(reply) { r =>
            complete(r)
          }
        },
        pathPrefix("close" / Remaining) { name =>
          val reply: Future[Reply] = tasks.ask(ref => TaskManager.Close(name, ref))
          onSuccess(reply) { r =>
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, r.msg))
          }
        },
        pathPrefix("info" / Remaining) { name =>
          val reply: Future[Reply] = tasks.ask(ref => TaskManager.Info(name, ref))
          onSuccess(reply) { r =>
            complete(r)
          }
        },
        pathSingleSlash {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1>Names of all tasks:</h1><ol><li>es-com</li><li>dummy</li></ol>"))
        }

    )

    val bindingFuture = Http().newServerAt("localhost", 8181).bind(route)
    println(s"Server online at http://localhost:8181/\n Press RETURN to stop...")
    StdIn.readLine()
    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }
}
