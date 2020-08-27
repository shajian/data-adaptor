package com.qcm.utils

import com.qcm.dal.mybatis.MybatisClient
import com.qcm.es.entity.EsComEntity
import com.qcm.task.maintask.ComUtil
import scala.jdk.CollectionConverters._
import scala.collection.mutable

object ComInfoFill {
  def fillEsCom(c: EsComEntity): Unit = {
    val dtl = MybatisClient.getCompanyDtl(c.code)
    c.status = ComUtil.getCompanyStatus(dtl.od_ext)
  }

  def batchFillEsCom(cs: List[EsComEntity]): Unit = {
    if (cs.isEmpty) return
    val sb = new mutable.StringBuilder().append("select od_oc_code, od_ext from OrgCompanyDtl where od_oc_code in ('")
    sb.append(cs.map(_.code).mkString("', '")).append("')")
    val ls = MybatisClient.selectMany(sb.toString())
    val map = ls.asScala.map(_.asInstanceOf[mutable.Map[String, String]]).map(l => (l("od_oc_code"), l("od_ext"))).toMap
    cs.foreach(c => c.status = ComUtil.getCompanyStatus(map(c.code)))
  }
}
