package com.qianzhan.qichamao.dal.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.qianzhan.qichamao.entity.MongoCompany;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.DbConfigBus;
import org.bson.BasicBSONObject;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;

public class MongodbClient {
    private static MongoClient client;
    private static String CompanyDatabase;
    private static String CompanyCollection;

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
        CompanyCollection = DbConfigBus.getDbConfig_s("mongo.company.col", "dtl");
        MongoClientSettings settings = MongoClientSettings.builder()
                .credential(MongoCredential.createCredential(user, db, pass.toCharArray()))
//                .applyToSslSettings(builder -> builder.enabled(true))
                .applyToClusterSettings(builder ->
                        builder.hosts(addresses))
                .build();
        client = MongoClients.create(settings);
    }

    public static List<MongoCompany> find(List<String> ids) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        List<MongoCompany> companies = new ArrayList<>(ids.size());
        for (Document doc : collection.find(in("_id", ids))) {
            companies.add(BeanUtil.doc2Obj(doc, MongoCompany.class));
        }
        return companies;
    }

    public static MongoCompany find(String id) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        Document doc = collection.find(eq("_id", id)).first();
        return BeanUtil.doc2Obj(doc, MongoCompany.class);
    }


    public static Document findDoc(String id) {
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("col");

        Document doc = collection.find(eq("_id", new ObjectId(id))).first();
        return doc;
    }
    public static boolean delete(String id) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        DeleteResult result = collection.deleteOne(eq("_id", id));
        return result.wasAcknowledged();
    }

    public static long delete(List<String> ids) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        DeleteResult result = collection.deleteMany(in("_id", ids));
        return result.getDeletedCount();
    }

    public static boolean upsert(Document doc) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        UpdateResult result = collection.replaceOne(
                eq("_id", doc.getString("_id")), doc, new ReplaceOptions().upsert(true));
        return result.wasAcknowledged();
    }

    public static boolean upsert(List<Document> docs) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        List<ReplaceOneModel<Document>> models = new ArrayList<>(docs.size());
        for (Document doc : docs) {
            models.add(new ReplaceOneModel(
                    eq("_id", doc.getString("_id")), doc, new ReplaceOptions().upsert(true)));
        }
        BulkWriteResult result = collection.bulkWrite(models);
        return result.wasAcknowledged();
    }

    public static void insert(List<Document> docs) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        collection.insertMany(docs);
    }


    public static void insert(Document doc) {
        MongoDatabase db = client.getDatabase(CompanyDatabase);
        MongoCollection<Document> collection = db.getCollection(CompanyCollection);
        collection.insertOne(doc);
    }
}
