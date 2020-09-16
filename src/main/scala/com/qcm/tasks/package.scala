package com.qcm

import scala.collection.mutable

package object tasks {
  private val taskMap: mutable.Map[String, Task] = mutable.Map.empty
  def register(name: String, task: Task): Unit = taskMap.put(name, task)
  def get(name: String): Option[Task] = taskMap.get(name)

  def start(name: String): Boolean = taskMap.get(name) match {
    case Some(task) =>
      task.run()
      true
    case _ => false
  }

  def close(name: String): Boolean = taskMap.get(name) match {
    case None => false
    case Some(task) =>
      task.closeAsync()
      true
  }
}
