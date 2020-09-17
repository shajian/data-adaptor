package com.qcm

import com.qcm.utils.Reflect

import scala.collection.{immutable, mutable}

package object tasks {

  private val taskClasses: immutable.Map[String, String] = Map(
    "es-com" -> "ESComTask",
    "dummy" -> "DummyTask"
  )


  def get(name: String): Option[Task] = taskClasses.get(name) match {
    case Some(clazz) =>
      try {
        val task: Task = Reflect._new("com.qcm.tasks."+clazz)
        Some(task)
      } catch {
        case _: Throwable => None
      }
    case _ => None
  }
}
