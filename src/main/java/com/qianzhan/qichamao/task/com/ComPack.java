package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.MongoComDtl;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;

import java.util.HashMap;
import java.util.Map;

public class ComPack {
    public EsCompany e_com;
    public MongoComDtl m_com;
    public RedisCompanyIndex r_com;
    public ArangoCpPack a_com;


    private static Map<String, int[]> tts = new HashMap<>();

    public static void registerTasktype(String key, int[] tasks) {
        tts.put(key, tasks);
    }
    public ComPack(String key) {
        this(tts.get(key));
    }

    public ComPack(int[] tasks) {
        for (int tt : tasks) {
            if ((tt & TaskType.es.getValue()) != 0) {
                e_com = new EsCompany();
            }
            if ((tt & TaskType.mongo.getValue()) != 0) {
                m_com = new MongoComDtl();
            }
            if ((tt & TaskType.redis.getValue()) != 0) {
                r_com = new RedisCompanyIndex();
            }
            if ((tt & TaskType.arango.getValue()) != 0) {
                a_com = new ArangoCpPack();
            }
        }
    }
}
