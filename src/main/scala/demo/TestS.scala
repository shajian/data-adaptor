package demo

import java.io.File

import com.qcm.tasks.Task
import com.qcm.utils.Reflect

object TestS extends App {
  val dummy: Task = Reflect._new("com.qcm.tasks.DummyTask")
  println(s"dynamically new a dummy whose name is: ${dummy.name}")
  val escom: Task = Reflect[Task]("com.qcm.tasks.ESUpdateLogTask")
  println(s"dynamically call ESUpdateLogTask.apply to new an instance whose name is ${escom.name}")

//  Reflect.test()
}
