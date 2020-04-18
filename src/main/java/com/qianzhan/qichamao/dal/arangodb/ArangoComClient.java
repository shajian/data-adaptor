package com.qianzhan.qichamao.dal.arangodb;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.DocumentCreateOptions;
import com.qianzhan.qichamao.util.DbConfigBus;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

@Getter
public class ArangoComClient {
    private String dbName = "company";
    private String graphName = "cmap";
    private String vertexCollName = "cp";
    private String edgeCollName = "cpr";
    private ArangoDB client;
    public ArangoComClient() {
        client = ArangoClient.getClient();
    }

    private static ArangoComClient singleton;
    public static ArangoComClient getSingleton() {
        if (singleton == null) {
            singleton = new ArangoComClient();
        }
        return singleton;
    }

    /**
     * initialize a graph
     * after creating a graph, two collections for vertex and edge are automatically created.
     */
    public void initGraph() {
        try {
            ArangoDatabase db = client.db(dbName);
            if (!db.exists()) {
                client.createDatabase(dbName);
                db = client.db(dbName);
            }

            ArangoGraph graph = db.graph(graphName);
            if (!graph.exists()) {
                EdgeDefinition ed = new EdgeDefinition().collection(edgeCollName)
                        .from(vertexCollName).to(vertexCollName);
                db.createGraph(graphName, Arrays.asList(ed));
            }

            // check
            ArangoCollection vds = db.collection(vertexCollName);
            ArangoCollection eds = db.collection(edgeCollName);
            if (!vds.exists() || !eds.exists()) {
                System.out.println("no vertex collection after creating a graph");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param coll 1 -> vertexCollName
     *             2 -> edgeCollName
     */
    public void truncate(int coll) {
        ArangoDatabase db = client.db(dbName);
        if (db.exists()) {
            String name = coll == 1 ? vertexCollName : edgeCollName;
            ArangoCollection collection = db.collection(name);
            if (collection.exists()) {
                collection.truncate();
            }
        }
    }

    /**
     * drop graph.
     * @param dropCollections whether drop collections used in this graph. Note that if set to true,
     *                        collections only used in this graph are dropped.
     */
    public void dropGraph(boolean dropCollections) {
        ArangoDatabase db = client.db(dbName);
        if (db.exists()) {
            ArangoGraph graph = db.graph(graphName);
            if (graph.exists()) {
                graph.drop(dropCollections);
            }
        }
    }

    public void bulkInsert_VD(List<BaseDocument> vds) {
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(vertexCollName);
        coll.insertDocuments(vds, new DocumentCreateOptions());
    }

    public void bulkInsert_ED(List<BaseDocument> eds) {
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(edgeCollName);
        coll.insertDocuments(eds, new DocumentCreateOptions());
    }
}
