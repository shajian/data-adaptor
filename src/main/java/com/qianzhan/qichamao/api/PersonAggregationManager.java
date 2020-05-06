package com.qianzhan.qichamao.api;

import com.qianzhan.qichamao.dal.arangodb.ArangoComClient;
import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.entity.MongoPersonAgg;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.Cryptor;

import java.util.List;

public class PersonAggregationManager {
    private static ArangoComClient client = ArangoComClient.getSingleton();

    public static void addManualPersonAgg(String name, String code1, String code2, byte status) {
        String name_md5 = Cryptor.md5(name);
        String start_id = "cp/" + code1 + name_md5;
        String end_id = "cp/" + code2 + name_md5;
        if (status == 1) { // combine
            List<List<String>> olds = client.updateFrom(start_id, end_id);
            client.delete_VD(end_id.split("/")[1]);
        } else if (status == 2) {       // split
            List<String> froms1 = client.neighbours("cp/"+code1);
            List<String> froms2 = client.neighbours("cp/"+code2);
            String id1 = null;
            String id2 = null;
            for (String from1 : froms1) {
                if (from1.endsWith(name_md5)) {
                    id1 = from1;
                    break;
                }
            }
            for (String from2 : froms2) {
                if (from2.endsWith(name_md5)) {
                    id2 = from2;
                    break;
                }
            }

            if (id1 != null && id1.equals(id2)) {
                String code = id1.substring(3, 12);
                if (code.equals(code1)) {
                    List<List<String>> olds = client.updateFrom(end_id, id1);
                } else if (code.equals(code2)) {
                    List<List<String>> olds = client.updateFrom(start_id, id2);
                }
            }
        }
        // save to Mongodb
        MongoPersonAgg agg = new MongoPersonAgg();
        agg.status = status;
        agg.code1 = code1;
        agg.code2 = code2;
        agg.name = name;
        agg._id = code1 + code2 + name_md5;
        MongoClientRegistry.client("agg").upsert(BeanUtil.obj2Doc(agg));
    }
}
