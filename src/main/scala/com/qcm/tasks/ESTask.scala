package com.qcm.tasks

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.qcm.actor.{BatchEsComFilled, BatchFillEsCom, ComDtl, Command, ESCom, ESComStart}
import com.qcm.dal.mybatis.MybatisClient
import com.qcm.es.entity.{EsComEntity, EsUpdateLogEntity}
import com.qcm.es.repository.{EsBaseRepository, EsComRepository}
import com.qcm.tasks.ESComTask.repository
import com.qcm.utils.{BaseConfigBus, ComInfoFill, TypeConverter}

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

abstract class ESTask[T](name: String, configFile: String) extends BaseTask(name, configFile)
//                 with ActorTask[Command]
{
  val repository: EsBaseRepository[T]
  def state1_pre(): Unit = {
    // build ES schema here
    if (!repository.exists()) {
      val index = repository.getIndexMeta.index()
      println(s"ES index $index is not existed, it will be created...")
      repository.map()
    }
  }
//  val system = ActorSystem(act(), name)


//  def act(): Behavior[Command] = Behaviors.setup { context =>
//    val dtl = context.spawn(ComDtl(), "com_dtl")
//    Behaviors.receiveMessage { message =>
//      message match {
//        case ESComStart(cp, cs) =>
//          dtl ! BatchFillEsCom(cp, cs, context.self)
//        case BatchEsComFilled(cp, cs) =>
//          repository.index(cs.asJava)
//          MybatisClient.updateCheckpoint(checkpointName, cp)
//      }
//      Behaviors.same
//    }
//  }
}

object ESComTask extends ESTask[EsComEntity]("ESCom", "config/Task_ES_Com.txt") {
  val repository = EsComRepository.singleton()
  def prepare(): Unit = {
    state_pres(0) = state1_pre
    state_inners(0) = state1_inner
  }


  def state1_inner(checkpoint: Int): (Boolean, Int) = {
    val companies = MybatisClient.getCompanies(checkpoint, batch).asScala
    if (companies.isEmpty) return (false, checkpoint)
    val entities = TypeConverter.orgCompanyList2EsComEntity(companies.toList)
    ComInfoFill.batchFillEsCom(entities)
    repository.index(entities.asJava)
    (true, companies.last.oc_id)
  }
}
