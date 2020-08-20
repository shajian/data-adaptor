package com.qcm.test.com.qcm.utils

import scala.io.Source

object IO {
  def readLines(path: String) = Source.fromFile(path).getLines()
}
