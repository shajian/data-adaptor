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

    /**
     * initialize a graph
     * after creating a graph, two collections for vertex and edge are automatically created.
     */
    public void initGraph() throws Exception {
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
        if (!vds.exists()) {
            throw new Exception("no vertex collection after creating a graph");
        }
        ArangoCollection eds = db.collection(edgeCollName);
        if (!eds.exists()) {
            throw new Exception("no edge collection after creating a graph");
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
