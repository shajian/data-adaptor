package com.qianzhan.qichamao.es;

import lombok.Getter;
import lombok.Setter;

/**
 * because we only read (but not write in any situation) from company es legacy index,
 * some fields we are interesting in are listed here.
 */
@Setter@Getter
@EsIndexMeta(index = "company_v1", id = "oc_code", type = "company")
public class EsCompanyEntityLegacy {
    private String oc_code;
    private String oc_name;
    private String oc_area;
    private byte oc_status;
}
