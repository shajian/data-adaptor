package com.qcm.es.repository;

import com.qcm.es.entity.EsComEntity;

public class EsComRepository extends EsBaseRepository<EsComEntity> {
    private static EsComRepository _singleton;
    public static EsComRepository singleton() {
        if (_singleton == null) _singleton = new EsComRepository();
        return _singleton;
    }
}
