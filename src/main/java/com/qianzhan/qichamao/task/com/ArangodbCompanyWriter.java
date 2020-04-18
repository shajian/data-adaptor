package com.qianzhan.qichamao.task.com;

import com.arangodb.entity.BaseDocument;
import com.qianzhan.qichamao.dal.arangodb.ArangoComClient;

import java.util.ArrayList;
import java.util.List;

public class ArangodbCompanyWriter {
    private static ArangoComClient client = ArangoComClient.getSingleton();
    public static void bulkInsert(String tasks_key) {
        List<ComPack> cps = SharedData.getBatch(tasks_key);
        ArrayList<BaseDocument> docs = new ArrayList<BaseDocument>(cps.size());
        for(ComPack cp : cps) {
            docs.add(cp.a_com.com.to());
        }
        try {
            client.bulkInsert_VD(docs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void upsert(String tasks_key) {
        List<ComPack> cps = SharedData.getBatch(tasks_key);
    }
}
