package com.qcm.es;

import org.elasticsearch.client.RestHighLevelClient;

public class EsMapper<T> {
    public void map(RestHighLevelClient client, String index) {
        this.getClass().getGenericSuperclass();
    }
}
