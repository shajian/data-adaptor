package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mongodb.MongodbClient;
import com.qianzhan.qichamao.entity.MongoCompany;
import com.qianzhan.qichamao.util.BeanUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongodbCompanyWriter extends BaseWriter {

    public MongodbCompanyWriter() throws Exception {
        super("");
    }
    /**
     * for state==1, this task is attached to EsCompanyWriter, and
     * only provides some APIs to implement writing into Mongodb.
     */

    public static void write2Db(List<MongoCompany> companies) {
        List<Document> docs = new ArrayList<>(companies.size());
        for (MongoCompany es_c : companies) {
            docs.add(BeanUtil.obj2Doc(es_c));
        }
        MongodbClient.insert(docs);
    }

    public static void write2Db(String tasks_key) {
        List<ComPack> cps = SharedData.getBatch(tasks_key);
        List<Document> docs = new ArrayList<>(cps.size());
        for (ComPack cp: cps) {
            docs.add(BeanUtil.obj2Doc(cp.m_com));
        }
        MongodbClient.insert(docs);
    }
}
