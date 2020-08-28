package com.qcm.utils

import java.util.Date

import com.qcm.entity.{OrgCompanyList, OrgCompanyUpdateMeta}
import com.qcm.es.entity.{EsComEntity, EsCompanyEntity, EsUpdateLogEntity}
import com.qcm.task.maintask.TaskType

object TypeConvert {
  def orgCompanyList2EsComEntity(cs: List[OrgCompanyList]): List[EsComEntity] =
    cs.map(c => orgCompanyList2EsComEntity(c))

  def orgCompanyList2EsComEntity(c: OrgCompanyList): EsComEntity = {
    val e = new EsComEntity
    e.area = c.oc_area
    e.code = c.oc_code
    e.name = c.oc_name.trim
    e
  }

  def updateMeta2EsComEntity(m: OrgCompanyUpdateMeta): EsComEntity = {
    val e = new EsComEntity
    e.code = m.field_values
    e
  }

  def updateMeta2UpdateLog(task_name: String, meta: OrgCompanyUpdateMeta): EsUpdateLogEntity = {
    val entity = new EsUpdateLogEntity
    entity.create_time = meta.create_time
    entity.read_time = new Date
    entity.field_names = meta.field_names
    entity.field_values = meta.field_values
    entity.table_name = meta.table_name
    entity.tbl_id = meta.id
    entity.task_name = task_name
    entity.md5()
    entity
  }
}
