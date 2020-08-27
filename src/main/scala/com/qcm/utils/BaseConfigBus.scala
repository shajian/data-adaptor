package com.qcm.utils

import scala.collection.mutable
import scala.io.Source


class BaseConfigBus(map: mutable.Map[String, String]) {
  def getInt(key: String, default: Int): Int = {
    val v = map.get(key)
    v match {
      case Some(s) => s.toInt
      case None => default
    }
  }

  def getBool(key: String, default: Boolean): Boolean = {
    val v = map.get(key)
    v match {
      case Some(s) => s.toBoolean
      case None => default
    }
  }

  def getFloat(key: String, default: Float): Float = {
    val v = map.get(key)
    v match {
      case Some(s) => s.toFloat
      case None => default
    }
  }

  def getInts(key: String): List[Int] = {
    val v = map.get(key)
    v match {
      case Some(s) => s.split(",").map(_.toInt).toList
      case None => List.empty
    }
  }

  def getString(key: String): Option[String] = map.get(key)
}

object BaseConfigBus {
  def apply(file: String): BaseConfigBus = {
    val map = mutable.Map.empty[String, String]
    Source.fromFile(file).getLines().filterNot(_.startsWith("#"))
      .map(_.trim).filterNot(_.isEmpty).map(l => l.split("="))
      .map(a => (a(0), a(1))).toMap
    new BaseConfigBus(map)
  }
}
