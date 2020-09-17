package com.qcm.actors

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.qcm.apps.Server

import scala.concurrent.duration._
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.es.entity.EsComEntity

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import com.qcm.es.repository.EsComRepository
import com.qcm.tasks.{Task}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}


object TaskManager {
  sealed class Command
  case class Start(name: String, replyTo: ActorRef[Server.Reply]) extends Command
  case class Close(name: String, replyTo: ActorRef[Server.Reply]) extends Command
  case class Info(name: String, replyTo: ActorRef[Server.Reply]) extends Command
//  case class Begin(name: String, replyTo: ActorRef[Command]) extends Command
//  case class Failed(message: String) extends Command
  case class Closed() extends Command

  val actors: mutable.Map[String, ActorRef[TaskActor.Command]] = mutable.Map.empty

  def apply(): Behavior[Command] = Behaviors.setup[Command] { context =>
    Behaviors.receiveMessage[Command] {
      case Start(name, replyTo) =>
        actors.get(name) match {
          case Some(actor) =>
            val task = com.qcm.tasks.get(name).get
            task.state match {
              case 1 => replyTo ! Server.Reply(s"task '$name' had already been started")
              case _ =>
                actor ! TaskActor.Start(context.self)
                replyTo ! Server.Reply(s"task '$name' is started successfully")
            }
          case _ =>
            com.qcm.tasks.get(name) match {
              case Some(task) =>
                val actor = context.spawn(TaskActor(task), task.name)
                actors.put(name, actor)
                actor ! TaskActor.Start(context.self)
                replyTo ! Server.Reply(s"task '$name' is started successfully")
              case _ => replyTo ! Server.Reply(s"task '$name' has not found: unknown task")
            }
        }
        Behaviors.same
      case Info(name, replyTo) =>
        com.qcm.tasks.get(name) match {
          case Some(task) =>
            replyTo ! Server.Reply(task.info)
          case _ => replyTo ! Server.Reply(s"task '$name' has not registered: unknown task")
        }
        Behaviors.same
      case Close(name, replyTo) =>
        actors.get(name) match {
          case Some(actor) =>
            val task = com.qcm.tasks.get(name).get
            task.closeAsync()
            implicit val timeout: Timeout = 5.seconds
            import Server.system
            val result: Future[Closed] = actor.ask[Closed](ref => TaskActor.Close(ref))
            result.onComplete {
              case Success(_) => replyTo ! Server.Reply(s"task '$name' is closed successfully.")
              case Failure(ex) => replyTo ! Server.Reply(s"failed to close task '$name', err: ${ex.getMessage}")
            }
          case _ => replyTo ! Server.Reply(s"task '$name' has not been started.")
        }
        Behaviors.same
    }
  }
}

object TaskActor {
  sealed class Command
  case class Start(replyTo: ActorRef[TaskManager.Command]) extends Command
  case class Close(replyTo: ActorRef[TaskManager.Closed]) extends Command
  def apply(task: Task): Behavior[Command] =
    Behaviors.receiveMessage[Command] {
      case Start(_) =>
        task.run()
        Behaviors.same
      case Close(replyTo) =>
        replyTo ! TaskManager.Closed()
        Behaviors.same

    }
}

//object CloseActor extends TaskActor {
//
//}