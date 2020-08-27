package com.qcm.actor

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.qcm.actor.ComDtl.fillEsCom
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.es.entity.EsComEntity

import scala.jdk.CollectionConverters._
import com.qcm.es.repository.EsComRepository

object ESCom {

  def apply() : Behavior[Command] =
}
