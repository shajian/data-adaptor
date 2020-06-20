package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.es.EsCompanyEntity;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;
import com.qianzhan.qichamao.graph.ArangoBusinessPack;
import com.qianzhan.qichamao.entity.MongoComDtl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ComPack {
    public EsCompanyEntity es;
    public MongoComDtl mongo;
    public RedisCompanyIndex redis;
    public ArangoBusinessPack arango;


    public TaskType task;

    public ComPack(TaskType task) {
        if (task == TaskType.es) es = new EsCompanyEntity();
        else if (task == TaskType.arango) arango = new ArangoBusinessPack();
        else if (task == TaskType.mongo) mongo = new MongoComDtl();
        else if (task == TaskType.redis) redis = new RedisCompanyIndex();
        this.task = task;
    }
}
