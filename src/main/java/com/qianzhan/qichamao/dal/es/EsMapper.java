package com.qianzhan.qichamao.dal.es;

import org.elasticsearch.client.RestHighLevelClient;

public class EsMapper<T> {
    public void map(RestHighLevelClient client, String index) {
        this.getClass().getGenericSuperclass();
    }
}
