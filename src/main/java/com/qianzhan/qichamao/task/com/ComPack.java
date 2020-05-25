package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;
import com.qianzhan.qichamao.graph.ArangoBusinessPack;
import com.qianzhan.qichamao.entity.MongoComDtl;

import java.util.HashMap;
import java.util.Map;

public class ComPack {
    public EsCompany es;
    public MongoComDtl mongo;
    public RedisCompanyIndex redis;
    public ArangoBusinessPack arango;


    private static Map<String, int[]> tts = new HashMap<>();

    public static void registerTasktype(String key, int[] tasks) {
        tts.put(key, tasks);
    }
    public ComPack (String key) throws Exception {
        this(tts.get(key));
    }

    public ComPack(int[] tasks) throws Exception {
        for (int tt : tasks) {
            if ((tt & TaskType.es.getValue()) != 0) {
                es = new EsCompany();
            }
            if ((tt & TaskType.mongo.getValue()) != 0) {
                mongo = new MongoComDtl();
            }
            if ((tt & TaskType.redis.getValue()) != 0) {
                redis = new RedisCompanyIndex();
            }
            if ((tt & TaskType.arango.getValue()) != 0) {
                arango = new ArangoBusinessPack();
            }
        }
    }
}
