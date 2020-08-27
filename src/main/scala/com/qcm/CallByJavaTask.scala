package com.qcm

import akka.actor.typed.ActorSystem
import com.qcm.actor.{Command, ESCom}
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.entity.OrgCompanyList
import com.qcm.es.entity.EsCompanyEntity

object ESComTask {
  val esCom: ActorSystem[Command] = ActorSystem(ESCom(), "ESCom")
  def esComState1(entities: Seq[EsCompanyEntity]): Unit = {
    if (entities.isEmpty) return
    
  }

}
