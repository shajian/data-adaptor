package com.qcm.utils

import com.qcm.tasks.ESUpdateLogTask

import scala.reflect.runtime.{currentMirror => m, universe => ru}
import scala.collection.mutable

object Reflect {
  private val objectCache: mutable.Map[String, Any] = mutable.Map.empty
  private val instanceCache: mutable.Map[String, Any] = mutable.Map.empty
  private val methodCache: mutable.Map[String, ru.MethodMirror] = mutable.Map.empty

  def test(): Unit = {
    val t = ru.typeOf[ESUpdateLogTask]
    println("succeed")
  }
  def new_[T](fullname: String): T = instanceCache.get(fullname) match {
    case Some(inst) => inst.asInstanceOf[T]
    case _ =>
      val inst = Class.forName(fullname).newInstance().asInstanceOf[T]
      instanceCache.put(fullname, inst)
      inst
  }

  def object_[T](fullname: String): T = objectCache.get(fullname) match {
    case Some(obj) => obj.asInstanceOf[T]
    case _ =>
      val clazz = Class.forName(fullname + "$")
      val obj = clazz.getField("MODULE$").get(clazz).asInstanceOf[T]
      objectCache.put(fullname, obj)
      obj
  }

  def _object[T](fullname: String): T = objectCache.get(fullname) match {
    case Some(obj) => obj.asInstanceOf[T]
    case _ =>
//      val m = ru.runtimeMirror(getClass.getClassLoader)
      val clazz = m staticModule fullname
      val objMirror = m reflectModule clazz
      val obj = objMirror.instance
      val t = obj.asInstanceOf[T]
      objectCache.put(fullname, obj)
      t
  }

  def _new[T: ru.TypeTag](fullname: String): T = instanceCache.get(fullname) match {
    case Some(inst) => inst.asInstanceOf[T]
    case _ =>
//      val m = ru.runtimeMirror(getClass.getClassLoader)
      val clazz = m staticClass fullname
//      val clazz = ru.typeOf[T].typeSymbol.asClass
      val cm = m.reflectClass(clazz.asClass)
      val ctor = cm.reflectConstructor(clazz.typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod)
      val inst = ctor().asInstanceOf[T]
      instanceCache.put(fullname, inst)
      inst
  }

  def apply[T](fullname: String, args: Any*): T = methodCache.get(fullname) match {
    case Some(method) => method.apply(args).asInstanceOf[T]
    case _ =>
//      val m = ru.runtimeMirror(getClass.getClassLoader)
      val clazz = m staticModule fullname
      val objMirror = m reflectModule clazz
      val obj = objMirror.instance
      val t = obj.asInstanceOf[T]
      val apply = (clazz.typeSignature decl ru.TermName("apply")).asMethod
      val instanceMirror = m reflect obj
      val applyMirror = instanceMirror reflectMethod apply
      methodCache.put(fullname, applyMirror)
      applyMirror.apply(args).asInstanceOf[T]
  }
}
