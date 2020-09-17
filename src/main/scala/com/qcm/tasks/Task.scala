package com.qcm.tasks

import akka.actor.typed.{ActorRef, Behavior}
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.utils.{BaseConfigBus, Constants}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.StdIn
import scala.runtime.Nothing$
import scala.util.matching.Regex

//object TaskType extends Enumeration {
//  type TaskType = Value
//  val es, arango, mongo, redis = Value
//}

object TaskImplicitParams {
  implicit val mtds: Int = 3
}




trait Task {
  val config: BaseConfigBus
  val name: String
  val methods: Int
  def batch = config.getInt("batch", 100)
  def method = config.getInt("method", 1) - 1

  def log(msg: String): Unit
  def prepare(): Unit
  def gen_mtd_pre_posts = (0 until methods).map(_ => () => {}).toArray
  def gen_mtd_inners = (0 until methods).map(_ => (_: Int) => (false, 0))
  val mtd_before: Array[() => Unit]
  val mtd_after: Array[() => Unit]
  val mtd_inner: Array[Int => (Boolean, Int)]
  def run(): Unit

  // 0: initial; 1: running; (2: paused/suspended;[deprecated]) 3:closing; 4: closed
  protected var _state: Int = 0
  def state: Int = _state
  implicit def bool2Int(b: Boolean) = if (b) 1 else 0
  val state_inners = (0 until methods).map(_ => (_: Int) => (true, 0)).toArray

  def info: String

  def closeAsync(): Unit = _state = 3

  protected def close(): Unit = _state match {
    case 4 => println("program has been closed safely.")
    case 3 => println("program is closing, please wait for a min.")
    case _ => {
      _state = 3
      println("program is closing, please wait for 5 seconds...")
      sleep(10)
    }
  }

  private def sleep(i: Int): Unit = (_state == 4 || i == 0) match {
    case true => {}
    case _ => {
      Thread.sleep(500)
      sleep(i-1)
    }
  }
}

trait DBIO {
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



abstract class ComplexTask(file: String)(implicit mtds: Int) extends SimpleTask(file) with DBIO {
  assert(mtds>0, "number of total different executing methods must be larger than 0")
  override val methods = mtds
  val checkpointName = List(Constants.projectName, name) mkString "_"



  override def run(): Unit = {
    _state = 1
    sys addShutdownHook close
    prepare()
    mtd_before(method)()
    val checkpoint: Int = getCheckpoint match {
      case 0 => 0
      case cp => {
        StdIn.readLine(s"$checkpointName: checkpoint->$cp, reset it (Yy|Nn)?").toLowerCase
        match {
          case "y" => 0
          case _ => cp
        }
      }
    }
    mtd_loop(checkpoint)
    mtd_after(method)()
  }

  def mtd_loop(checkpoint: Int): Unit = _state match {
    case 3 | 4 => _state = 4            // task is (already) terminated manually, set flag `closed`
    case _ => mtd_inner(method)(checkpoint) match {
      case (false, _) => _state = 4    // task completed and exits normally, set flag `closed`
      case (true, cp) => {                // task has not finished and will goes into next iteration
        updateCheckpoint(cp)
        log(s"$name, checkpoint: $cp")
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
  private val _log: String => Unit = if (config.getBool("log_file", false))
    LoggerFactory.getLogger(name).info else println
  def log(msg: String) = _log(msg)

  def run(): Unit = {
    _state = 1
    sys addShutdownHook close
    prepare()
    mtd_before(method)()
    mtd_loop()
    mtd_after(method)()
  }

  def mtd_loop(): Unit = _state match {
    case 3 | 4 => _state = 4
    case _ => mtd_inner(method)(0)._1 match {
      case false => _state = 4
      case true =>
        log(s"$name, one iteration finishes.")
        mtd_loop()
    }
  }
}

//trait ActorTask[T] {
//  val system: ActorRef[T]
//}

class DummyTask extends Task {
  val config = new BaseConfigBus(mutable.Map.empty)
  val name = "dummy"
  val methods = 1

  val mtd_before = gen_mtd_pre_posts
  val mtd_after = gen_mtd_pre_posts
  val mtd_inner = gen_mtd_inners.toArray

  def info:String = s"dummy task: state->${_state}"

  override def log(msg: String): Unit = DummyTask.logger.info(msg) //println("dummy task: logging sth.")

  override def prepare(): Unit = mtd_inner(0) = (_: Int) => {
    Thread.sleep(2000)
    (true, 0)
  }

  override def run(): Unit = {
    _state = 1
    sys addShutdownHook close
    prepare()
    mtd_before(method)()
    mtd_loop()
    mtd_after(method)()
  }

  def mtd_loop(): Unit = _state match {
    case 3 | 4 => _state = 4
    case _ => mtd_inner(method)(0)._1 match {
      case false => _state = 4
      case true =>
        log(s"$name, one iteration finishes.")
        mtd_loop()
    }
  }
}

object DummyTask {
  val logger = LoggerFactory.getLogger(this.getClass.getName)
}



