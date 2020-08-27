package com.qcm.es.search;

import com.qcm.es.entity.EsComEntity;
import com.qcm.es.repository.EsComRepository;

import java.util.List;

public class EsComTripleSearcher {
    public static List<EsComEntity> mget(List<String> codes) {
        return mget(codes.toArray(new String[1]));
    }

    public static List<EsComEntity> mget(String[] codes) {
        if (codes != null && codes.length > 0) return null;

        EsSearchBaseParam<EsComEntity> param = new EsSearchBaseParam<>();
        param.setIds(codes);
        return EsComRepository.singleton().mget(param);
    }
}
