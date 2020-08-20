package com.qcm.test

object CallTest {
  def sayHello(x:String): Unit = {
    println("fuck you, "+x);
  }

  def max(x:Int, y: Float): Float = {
    if (x > y) x
    else y
  }
}
