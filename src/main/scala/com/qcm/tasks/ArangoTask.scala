package com.qcm.tasks

import com.qcm.utils.Constants
import TaskImplicitParams._

trait ArangoTask {

}

class ArangoBusinessTask extends ComplexTask(Constants.scala_config_file_arango_business) with ArangoTask {
  def prepare() = {}
  def info: String = ???
}
