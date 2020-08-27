package com.qcm.task.maintask;

import com.qcm.entity.OrgCompanyContact;
import com.qcm.util.BeanUtil;
import com.qcm.util.MiscellanyUtil;
import com.qcm.dal.mongodb.MongoClientRegistry;
import com.qcm.dal.mongodb.MongodbClient;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.MongoComContact;
import com.qcm.entity.MongoComDtl;
import org.bson.Document;

import java.util.*;

public class MongoComTask extends BaseTask {

    public MongoComTask() throws Exception {
        super("config/Task_Mongo_Company.txt");
        checkpointName = "data-adaptor.mongo.company";
    }
    /**
     * for state==1, this task is attached to ESCompanyTask, and
     * only provides some APIs to implement writing into Mongodb.
     */

    public static void writeDtl2Db(List<MongoComDtl> companies) {
        List<Document> docs = new ArrayList<>(companies.size());
        for (MongoComDtl es_c : companies) {
            docs.add(BeanUtil.obj2Doc(es_c));
        }
        MongoClientRegistry.client(MongoClientRegistry.CollName.dtl).insert(docs);
    }

    public static void writeDtl2Db(TaskType task) {
        List<ComPack> cps = SharedData.getBatch(task);
        List<Document> docs = new ArrayList<>(cps.size());
        for (ComPack cp: cps) {
            docs.add(BeanUtil.obj2Doc(cp.mongo));
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
