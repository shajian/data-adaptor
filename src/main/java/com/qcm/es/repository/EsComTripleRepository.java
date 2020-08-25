package com.qcm.es.repository;

import com.qcm.es.entity.EsComTripleEntity;

public class EsComTripleRepository extends EsBaseRepository<EsComTripleEntity> {
    private static EsComTripleRepository _singleton;
    public static EsComTripleRepository singleton() {
        if (_singleton == null) _singleton = new EsComTripleRepository();
        return _singleton;
    }
}
