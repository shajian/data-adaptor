package com.qcm.tasks

import akka.actor.typed.{ActorRef, Behavior}
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.utils.{BaseConfigBus, Constants}

import scala.io.StdIn
import scala.util.matching.Regex

//object TaskType extends Enumeration {
//  type TaskType = Value
//  val es, arango, mongo, redis = Value
//}

object TaskImplicitParams {
  implicit val mtds: Int = 3
}

trait CloseTask {
  protected var closing = false
  protected var closed  = true

  protected def close(): Unit = closed match {
    case true => println("program has been closed safely.")
    case _ => {
      closing = true
      println("program is closing, please wait for 5 seconds...")
      sleep(10)
    }
  }

  private def sleep(i: Int): Unit = (closed || i == 0) match {
    case true => Unit
    case _ => {
      Thread.sleep(500)
      sleep(i-1)
    }
  }
}

trait Task extends CloseTask {
  val config: BaseConfigBus
  val name: String
  val methods: Int
  def batch = config.getInt("batch", 100)
  def method = config.getInt("method", 1) - 1

  def prepare(): Unit
  def gen_mtd_pre_posts = (0 until methods).map(_ => () => {}).toArray
  def gen_mtd_inners = (0 until methods).map(_ => (_: Int) => (false, 0))
  val mtd_before: Array[() => Unit]
  val mtd_after: Array[() => Unit]
  val mtd_inner: Array[Int => (Boolean, Int)]
  def start(): Unit
  implicit def bool2Int(b: Boolean) = if (b) 1 else 0
  val state_inners = (0 until methods).map(_ => (_: Int) => (true, 0)).toArray
}

trait DBIOTask {
  val checkpointName: String
  def getCheckpoint = MybatisClient.getCheckpoint(checkpointName) match {
    case cp if cp >= 0 => cp
    case _ => {
      MybatisClient.insertCheckpoint0(checkpointName)
      0
    }
  }
  def updateCheckpoint(checkpoint: Int) = MybatisClient.updateCheckpoint(checkpointName, checkpoint)
}



abstract class ComplexTask(file: String)(implicit mtds: Int) extends SimpleTask(file) with DBIOTask {
  assert(mtds>0, "number of total different executing methods must be larger than 0")
  override val methods = mtds
  val checkpointName = List(Constants.projectName, name) mkString "_"



  override def start(): Unit = {
    sys addShutdownHook close
    prepare()
    mtd_before(method)()
    val checkpoint = getCheckpoint match {
      case 0 => 0
      case cp => {
        StdIn.readLine(s"$checkpointName: checkpoint->$checkpoint, reset it (Yy|Nn)?").toLowerCase
        match {
          case "y" => 0
          case _ => cp
        }
      }
    }
    mtd_loop(checkpoint)
    mtd_after(method)()
  }

  def mtd_loop(checkpoint: Int): Unit = closing match {
    case true => closed = true
    case _ => mtd_inner(method)(checkpoint) match {
      case (false, _) => closed = true
      case (true, cp) => {
        updateCheckpoint(cp)
        mtd_loop(cp)
      }
    }
  }
}

abstract class SimpleTask(file: String) extends Task {
  val config = BaseConfigBus(file)
  val methods = 1
  val name = file.split("_").last
  val mtd_before = gen_mtd_pre_posts
  val mtd_after = gen_mtd_pre_posts
  val mtd_inner = gen_mtd_inners.toArray

  def start(): Unit = {
    sys addShutdownHook close
    prepare()
    mtd_before(method)()
    mtd_loop()
    mtd_after(method)()
  }

  def mtd_loop(): Unit = closing match {
    case true => closed = true
    case _ => mtd_inner(method)(0)._1 match {
      case false => closed = true
      case true => mtd_loop()
    }
  }
}

trait ActorTask[T] {
  val system: ActorRef[T]
}



