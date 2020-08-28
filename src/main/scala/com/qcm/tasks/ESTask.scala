package com.qcm.tasks

import com.qcm.dal.mybatis.MybatisClient
import com.qcm.entity.OrgCompanyUpdateMeta
import com.qcm.es.entity.{EsComEntity, EsUpdateLogEntity}
import com.qcm.es.repository.{EsBaseRepository, EsComRepository, EsUpdateLogRepository}
import com.qcm.utils.{Constants, ESComFill, TypeConvert}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import TaskImplicitParams._


//                 with ActorTask[Command]


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

trait ESTask[T] {
  val repository: EsBaseRepository[T]
  def mtd0_before(): Unit = {
    // build ES schema here
    if (!repository.exists()) {
      val index = repository.getIndexMeta.index()
      println(s"ES index $index is not existed, it will be created...")
      repository.map()
    }
  }

}

object ESComTask extends ComplexTask(Constants.scala_config_file_es_com) with ESTask[EsComEntity] {
  val repository = EsComRepository.singleton()

//  val filter_outs = config.getString("filter_out") match {
//    case None => Array.empty
//    case Some(s) => s.split("\\s").map(p => new Regex(p))
//  }

  def prepare(): Unit = {
    mtd_before(0) = mtd0_before
    mtd_inner(0) = mtd0_inner
  }


  def mtd0_inner(checkpoint: Int): (Boolean, Int) = {
    val companies = MybatisClient.getCompanies(checkpoint, batch).asScala
    if (companies.isEmpty) return (false, checkpoint)
    val entities = TypeConvert.orgCompanyList2EsComEntity(companies.toList)
    ESComFill.batchFillStatus(entities)
    repository.index(entities.asJava)
    (true, companies.last.oc_id)
  }

  def mtd2_inner(checkpoint: Int): (Boolean, Int) =
    MybatisClient.getCompanyUpdateMeta(checkpoint, batch).asScala match {
      case metas if !metas.isEmpty => {
        val newMetas = mutable.Map.empty[String, OrgCompanyUpdateMeta]
        for (m <- metas) {
          val id = m.table_name + m.field_names + m.field_values
          newMetas += (id -> m)
        }
        val logs = newMetas.values.map(v => TypeConvert.updateMeta2UpdateLog(name, v))
        val groups = newMetas.values.partition(v => v.table_name == Constants.orgCompanyList)
        val first = groups._1.map(c => TypeConvert.updateMeta2EsComEntity(c)).toList
        val second = groups._2.map(c => TypeConvert.updateMeta2EsComEntity(c)).toList
        ESComFill.batchFillComTriple(first)
        ESComFill.batchFillStatus(second)
        repository.index((first++second).asJava)
        ESUpdateLogTask.repository.index(logs.toSeq.asJava)
        (true, metas.last.id)
      }
      case _ => (false, checkpoint)
    }

}

object ESUpdateLogTask extends SimpleTask(Constants.scala_config_file_update_log) with ESTask[EsUpdateLogEntity] {
  val repository = EsUpdateLogRepository.singleton()

  def prepare(): Unit = {
    mtd_before(0) = mtd0_before
    mtd_inner(0) = mtd0_inner
  }

  def mtd0_inner(checkpoint: Int): (Boolean, Int) = {
    // remove expired logs from ES
    _
  }
}
