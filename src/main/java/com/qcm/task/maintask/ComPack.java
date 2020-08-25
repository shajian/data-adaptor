package com.qcm.task.maintask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.entity.RedisCompanyIndex;
import com.qcm.graph.ArangoBusinessPack;
import com.qcm.entity.MongoComDtl;

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
