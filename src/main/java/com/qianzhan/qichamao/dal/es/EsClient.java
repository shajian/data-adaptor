package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.util.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EsClient {
    private static RestHighLevelClient client;
    private static Lock lock = new ReentrantLock();
    public static RestHighLevelClient getClient() {
        if (client != null) return client;
        lock.lock();
        init();
        lock.unlock();
        return client;
    }
    private static void init() {
        String[] urls = DbConfigBus.getDbConfig_ss("es.urls", null);
        HttpHost[] hosts = new HttpHost[urls.length];
        for(int i = 0; i < urls.length; ++i) {
            hosts[i] = HttpHost.create(urls[i]);
        }
        client = new RestHighLevelClient(RestClient.builder(hosts));
    }





}
