package com.qcm.entity;

import com.qcm.es.entity.EsCompanyEntity;
import lombok.Getter;
import lombok.Setter;

@Getter@Setter
public class EsCompanyMatch {
    private EsCompanyEntity com;
    private float confidence;
}
