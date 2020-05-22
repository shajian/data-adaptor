package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.OrgCompanyContact;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.dal.mongodb.MongodbClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.MongoComContact;
import com.qianzhan.qichamao.entity.MongoComDtl;
import org.bson.Document;

import java.util.*;

public class MongodbCompanyWriter extends BaseWriter {

    public MongodbCompanyWriter() throws Exception {
        super("config/MongoCompany.txt");
        checkpointName = "data-adaptor.mongo.company";
    }
    /**
     * for state==1, this task is attached to EsCompanyWriter, and
     * only provides some APIs to implement writing into Mongodb.
     */

    public static void writeDtl2Db(List<MongoComDtl> companies) {
        List<Document> docs = new ArrayList<>(companies.size());
        for (MongoComDtl es_c : companies) {
            docs.add(BeanUtil.obj2Doc(es_c));
        }
        MongoClientRegistry.client(MongoClientRegistry.CollName.dtl).insert(docs);
    }

    public static void writeDtl2Db(String tasks_key) {
        List<ComPack> cps = SharedData.getBatch(tasks_key);
        List<Document> docs = new ArrayList<>(cps.size());
        for (ComPack cp: cps) {
            docs.add(BeanUtil.obj2Doc(cp.m_com));
        }
        MongoClientRegistry.client(MongoClientRegistry.CollName.dtl).insert(docs);
    }

    protected void state3_pre() {
        MongodbClient client = MongoClientRegistry.client("contact");

        client.createIndex(true, "code", "contact");
    }

    protected boolean state3_inner() throws Exception {
        List<OrgCompanyContact> contacts = MybatisClient.getCompanyContactBatch(checkpoint, batch);
        if (contacts.size() == 0) return false;

        List<Document> cs = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (OrgCompanyContact contact : contacts) {
            checkpoint = contact.ID;
            contact.oc_contact = contact.oc_contact.replaceAll("\\s", "");
            if (MiscellanyUtil.isBlank(contact.oc_contact)) continue;
            if (!validateCode(contact.oc_code)) continue;
            if (contact.oc_status != 1) continue;

            MongoComContact c = new MongoComContact();
            c.code = contact.oc_code;
            c.contact = contact.oc_contact;
            c.type = contact.oc_type;
            c._id = c.code + c.contact;
            if (!ids.contains(c._id)) {
                ids.add(c._id);
                cs.add(BeanUtil.bean2Doc(c));
            }
        }
        try {
            MongoClientRegistry.client("contact").insert(cs);
        } catch (com.mongodb.MongoBulkWriteException | com.mongodb.MongoWriteException e) {
//            System.out.println("sequentially insert docs into mongodb");
            for (Document doc : cs) {
                try {
                    MongoClientRegistry.client("contact").insert(doc);
                } catch (com.mongodb.MongoWriteException ie) { }
            }
        }
        return true;
    }
}
