package com.qcm.utils

import com.qcm.entity.OrgCompanyList
import com.qcm.es.entity.{EsComEntity, EsCompanyEntity}

object TypeConverter {
  def orgCompanyList2EsComEntity(cs: List[OrgCompanyList]): List[EsComEntity] =
    cs.map(c => orgCompanyList2EsComEntity(c))

  def orgCompanyList2EsComEntity(c: OrgCompanyList): EsComEntity = {
    val e = new EsComEntity
    e.area = c.oc_area
    e.code = c.oc_code
    e.name = c.oc_name.trim
    e
  }
}
