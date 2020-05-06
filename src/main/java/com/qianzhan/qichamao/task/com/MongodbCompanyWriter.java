package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.dal.mongodb.MongodbClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.MongoComContact;
import com.qianzhan.qichamao.entity.MongoComDtl;
import com.qianzhan.qichamao.entity.OrgCompanyContact;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongodbCompanyWriter extends BaseWriter {

    public MongodbCompanyWriter() throws Exception {
        super("MongoCompany.txt");
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
        for (OrgCompanyContact contact : contacts) {
            checkpoint = contact.ID;
            if (MiscellanyUtil.isBlank(contact.oc_contact)) continue;
            if (!validateCode(contact.oc_code)) continue;

            MongoComContact c = new MongoComContact();
            c.code = contact.oc_code;
            c.contact = contact.oc_contact;
            c.type = contact.oc_type;
            c._id = c.code + c.contact;

            cs.add(BeanUtil.bean2Doc(c));
        }

        MongoClientRegistry.client("contact").insert(cs);
        return true;
    }
}
