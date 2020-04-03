package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.MongoCompany;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComPack {
    public EsCompany e_com;
    public MongoCompany m_com;
    public RedisCompanyIndex r_com;
    public ArangoCpPack a_com;


    private static Map<String, Integer> tts = new HashMap<>();

    public static void registerTasktype(String key, int tt) {
        tts.put(key, tt);
    }
    public ComPack(String key) {
        this(tts.get(key));
    }

    public ComPack(TaskType tt) {
        this(tt.getValue());
    }

    public ComPack(int tt) {
        if ((tt & TaskType.es.getValue()) != 0) {
            e_com = new EsCompany();
        }
        if ((tt & TaskType.mongo.getValue()) != 0) {
            m_com = new MongoCompany();
        }
        if ((tt & TaskType.redis.getValue()) != 0) {
            r_com = new RedisCompanyIndex();
        }
        if ((tt & TaskType.arango.getValue()) != 0) {
            a_com = new ArangoCpPack();
        }
    }
}
