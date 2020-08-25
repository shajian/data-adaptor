package com.qcm.es.search;

import com.qcm.es.entity.EsComTripleEntity;
import com.qcm.es.repository.EsComTripleRepository;

import java.util.List;

public class EsComTripleSearcher {
    public static List<EsComTripleEntity> mget(List<String> codes) {
        return mget(codes.toArray(new String[1]));
    }

    public static List<EsComTripleEntity> mget(String[] codes) {
        if (codes != null && codes.length > 0) return null;

        EsSearchBaseParam<EsComTripleEntity> param = new EsSearchBaseParam<>();
        param.setIds(codes);
        return EsComTripleRepository.singleton().mget(param);
    }
}
