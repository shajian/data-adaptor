package com.qcm.es.entity;

import com.qcm.es.EsIndexMeta;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
@EsIndexMeta(index = "com_triple", id = "code")
public class EsComTripleEntity {
    @EsFieldMeta
    private String name;
    @EsFieldMeta
    private String code;
    @EsFieldMeta
    private String area;
}
