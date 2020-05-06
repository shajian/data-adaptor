package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

@Getter@Setter
public class EsCompanyTripleMatch {
    private String oc_name;
    private String oc_code;
    private String oc_area;
    private byte oc_status;
    private double confidence;
}
