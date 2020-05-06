package com.qianzhan.qichamao.dal.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.operation.CreateIndexesOperation;
import com.qianzhan.qichamao.entity.MongoComContact;
import com.qianzhan.qichamao.entity.MongoComDtl;
import com.qianzhan.qichamao.entity.MongoPersonAgg;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.DbConfigBus;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class MongodbClient {
    private static MongoClient client;
    private static String CompanyDatabase;
//    private static String CompanyDtl;
//    private static String CompanyPerson;



    static {
        String[] hosts = DbConfigBus.getDbConfig_ss("mongos", null);
        List<ServerAddress> addresses = new ArrayList<>();
        for (String host : hosts) {
            String[] ipport = host.split(":");
            addresses.add(new ServerAddress(ipport[0], Integer.parseInt(ipport[1])));
        }
        String user = DbConfigBus.getDbConfig_s("mongo.user", "shajian");
        String pass = DbConfigBus.getDbConfig_s("mongo.pass", "shajian");
        String db = DbConfigBus.getDbConfig_s("mongo.db", "admin");

        CompanyDatabase = DbConfigBus.getDbConfig_s("mongo.company.db", "company");
//        CompanyDtl = DbConfigBus.getDbConfig_s("mongo.company.col.dtl", "dtl");
//        CompanyPerson = DbConfigBus.getDbConfig_s("mongo.company.col.person", "person");
        MongoClientSettings settings = MongoClientSettings.builder()
                .credential(MongoCredential.createCredential(user, db, pass.toCharArray()))
//                .applyToSslSettings(builder -> builder.enabled(true))
                .applyToClusterSettings(builder ->
                        builder.hosts(addresses))
                .build();
        client = MongoClients.create(settings);
    }

    protected String coll;

    public MongodbClient(String coll) {
        this.coll = coll;
    }

    public void createIndex(boolean unique, String... fields) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        collection.createIndex(Indexes.descending(fields), new IndexOptions().unique(unique));
    }

    public List<MongoComDtl> find(List<String> ids) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        List<MongoComDtl> companies = new ArrayList<>(ids.size());
        for (Document doc : collection.find(in("_id", ids))) {
            companies.add(BeanUtil.doc2Obj(doc, MongoComDtl.class));
        }
        return companies;
    }

    public List<MongoComContact> findBy(String field, List<String> values, String... includes) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        List<MongoComContact> cs = new ArrayList<>();
        for (Document doc : collection.find(in(field, values)).projection(Projections.include(includes))) {
            cs.add(BeanUtil.doc2Obj(doc, MongoComContact.class));
        }
        return cs;
    }

    public List<MongoPersonAgg> findBy(String field1, String field2, String value) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        List<MongoPersonAgg> ps = new ArrayList<>();
        for (Document doc : collection.find(or(eq(field1, value), eq(field2, value)))) {
            ps.add(BeanUtil.doc2Obj(doc, MongoPersonAgg.class));
        }
        return ps;
    }

    public MongoComDtl find(String id) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        Document doc = collection.find(eq("_id", id)).first();
        return BeanUtil.doc2Obj(doc, MongoComDtl.class);
    }


    public Document findTestDoc(String id) {
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("col");

        Document doc = collection.find(eq("_id", new ObjectId(id))).first();
        return doc;
    }
    public boolean delete(String id) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        DeleteResult result = collection.deleteOne(eq("_id", id));
        return result.wasAcknowledged();
    }

    public long delete(List<String> ids) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        DeleteResult result = collection.deleteMany(in("_id", ids));
        return result.getDeletedCount();
    }

    public boolean upsert(Document doc) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        UpdateResult result = collection.replaceOne(
                eq("_id", doc.getString("_id")), doc, new ReplaceOptions().upsert(true));
        return result.wasAcknowledged();
    }

    public boolean upsert(List<Document> docs) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        List<ReplaceOneModel<Document>> models = new ArrayList<>(docs.size());
        for (Document doc : docs) {
            models.add(new ReplaceOneModel(
                    eq("_id", doc.getString("_id")), doc, new ReplaceOptions().upsert(true)));
        }
        BulkWriteResult result = collection.bulkWrite(models);
        return result.wasAcknowledged();
    }

    public void insert(List<Document> docs) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        collection.insertMany(docs);
    }


    public void insert(Document doc) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(this.coll);
        collection.insertOne(doc);
    }
}
