package com.qcm.utils

import com.qcm.dal.mybatis.MybatisClient
import com.qcm.es.entity.EsComEntity
import com.qcm.task.maintask.ComUtil

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object ESComFill {
  def fillStatus(c: EsComEntity): Unit = {
    val dtl = MybatisClient.getCompanyDtl(c.code)
    c.status = ComUtil.getCompanyStatus(dtl.od_ext)
  }

  def batchFillStatus(cs: List[EsComEntity]): Unit = cs match {
    case Nil => {}
    case _ => {
      val sb = new mutable.StringBuilder().append("select od_oc_code, od_ext from OrgCompanyDtl where od_oc_code in ('")
      sb.append(cs.map(_.code).mkString("', '")).append("')")
      val ls = MybatisClient.selectMany(sb.toString())
      val map = ls.asScala.map(_.asInstanceOf[mutable.Map[String, String]]).map(l => (l("od_oc_code"), l("od_ext"))).toMap
      cs.foreach(c => c.status = map.get(c.code) match {
        case None => 0
        case Some(v) => ComUtil.getCompanyStatus(v)
      })
    }
  }

  def batchFillComTriple(cs: List[EsComEntity]): Unit = cs match {
    case Nil => {}
    case _ => {
      val sb = new mutable.StringBuilder().append("select oc_code, oc_name, oc_area from OrgCompanyList where oc_code in ('")
      sb.append(cs.map(_.code).mkString("', '")).append("')")
      val r = MybatisClient.selectMany(sb.toString())
      val map = r.asScala.asInstanceOf[mutable.Buffer[Map[String, String]]].map(m => (m("oc_code"), (m("oc_name"), m("oc_area")))).toMap
      cs.foreach(c => map.get(c.code) match {
        case None => {}
        case Some(e) => {
          c.area = e._2
          c.name = e._1
        }
      })
    }
  }
}
