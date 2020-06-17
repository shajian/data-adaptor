package com.qianzhan.qichamao.entity;

import com.qianzhan.qichamao.es.EsCompanyEntity;
import lombok.Getter;
import lombok.Setter;

@Getter@Setter
public class EsCompanyMatch {
    private EsCompanyEntity com;
    private float confidence;
}
