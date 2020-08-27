package com.qcm.es.entity;

import com.qcm.entity.ActorEntity;
import com.qcm.es.EsIndexMeta;

@EsIndexMeta(index = "com", id = "code")
public class EsComEntity {
    @EsFieldMeta
    public String name;
    @EsFieldMeta
    public String code;
    @EsFieldMeta
    public String area;
    @EsFieldMeta(type = EsFieldType.Byte)
    public byte status;
}
