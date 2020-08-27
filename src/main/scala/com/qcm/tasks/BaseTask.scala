package com.qcm.tasks

import akka.actor.typed.{ActorRef, Behavior}
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.tasks.ESTask.config
import com.qcm.utils.{BaseConfigBus, GlobalEnv}

import scala.io.StdIn
import scala.util.matching.Regex

case class DbAccessInfo(checkpoint: Int, checkpointName: String, batch: Int)

//object TaskType extends Enumeration {
//  type TaskType = Value
//  val es, arango, mongo, redis = Value
//}

abstract class BaseTask(val name: String, file: String) {
  val config = BaseConfigBus(file)
  val batch = config.getInt("batch", 100)
  val state = config.getInt("state", 1)
  val checkpointName = List(GlobalEnv.projectName, name).mkString("_")
  val filter_outs = config.getString("filter_out") match {
    case None => Array.empty
    case Some(s) => s.split("\\s").map(p => new Regex(p))
  }

  def prepare(): Unit
  val state_pres = (1 to 10).map(_ => () => {}).toArray
  val state_posts = (1 to 10).map(_ => () => {}).toArray
  val state_inners = (1 to 10).map(_ => (_: Int) => (true, 0)).toArray

  private var closing = false
  private var closed = false

  def start(): Unit = {
    prepare()
    sys.addShutdownHook(exitSafely)
    state_pres(state-1)()
    val checkpoint = MybatisClient.getCheckpoint(checkpointName)
    val cp = if (checkpoint < 0) {
      MybatisClient.insertCheckpoint0(checkpointName)
      0
    } else {
      val a = StdIn.readLine(s"$checkpointName: checkpoint->$checkpoint, reset it (Yy|Nn)?")
      if (a.toLowerCase == "y") 0 else checkpoint
    }
    state_iter(cp)
    state_posts(state-1)()
  }

  def state_iter(checkpoint: Int): Unit = {
    if (closing) {
      closed = true
    }
    val (status, cp) = state_inners(state-1)(checkpoint)

    status match {
      case false => if (state == 2) { Thread.sleep(1000*5); state_iter(cp) }
      case true => {
        MybatisClient.updateCheckpoint(checkpointName, cp)
        state_iter(cp)
      }
    }
  }

  private def exitSafely(): Unit = {
    closing = true
    println("program is closing, please wait for a minute...")
    sleep(10)
    if (!closed) {
      println("program will be closed after 5s")
      Thread.sleep(5000)
    }
  }

  private def sleep(i: Int): Unit = {
    if (closed || i == 0) return
    Thread.sleep(500)
    sleep(i+1)
  }
}

trait ActorTask[T] {
  val system: ActorRef[T]
}



