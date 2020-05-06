package com.qianzhan.qichamao.dal.arangodb;

import com.alibaba.fastjson.JSON;
import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.DocumentCreateOptions;
import com.qianzhan.qichamao.entity.ArangoCpED;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.ArangoCpPath;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        if (MiscellanyUtil.isArrayEmpty(vds)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(vertexCollName);
        coll.insertDocuments(vds, new DocumentCreateOptions());
    }

    public void bulkInsert_ED(List<BaseEdgeDocument> eds) {
        if (MiscellanyUtil.isArrayEmpty(eds)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(edgeCollName);
        coll.insertDocuments(eds, new DocumentCreateOptions());
    }


    public void bulkDelete_ED(List<String> keys) {
        if (MiscellanyUtil.isArrayEmpty(keys)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(edgeCollName);
        coll.deleteDocuments(keys);
    }

    public void delete_ED(String key) {
        if (MiscellanyUtil.isBlank(key)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(edgeCollName);
        coll.deleteDocument(key);
    }

    public List<String> neighbours(String to) {
        ArangoDatabase db = client.db(dbName);
        String aql = "FOR e in " + edgeCollName + " FILTER d._to == '%s' RETURN e._from";
        ArangoCursor<String> cursor = db.query(String.format(aql, to), String.class);
        List<String> froms = new ArrayList<>();
        while (cursor.hasNext()) {
            froms.add(cursor.next());
        }

        return froms;
    }

    public void bulkDelete_VD(List<String> keys) {
        if (MiscellanyUtil.isArrayEmpty(keys)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(vertexCollName);
        coll.deleteDocuments(keys);
    }

    public void delete_VD(String key) {
        if (MiscellanyUtil.isBlank(key)) return;
        ArangoDatabase db = client.db(dbName);
        ArangoCollection coll = db.collection(vertexCollName);
        coll.deleteDocument(key);
    }





    /**
     * delete edge according to `from` and `to`
     * return the deleted edges
     * @param from
     * @param to
     * @return
     */
    public List<ArangoCpED> delete_ED(String from, String to) {
        ArangoDatabase db = client.db(dbName);
        String aql = "FOR d IN " + edgeCollName
                + " FILTER d._from == '{0}' AND d._to == '{1}' REMOVE d IN "
                + edgeCollName + " RETURN OLD";
        aql = String.format(aql, from, to);
        ArangoCursor<BaseEdgeDocument> cursor = db.query(aql, BaseEdgeDocument.class);
        List<ArangoCpED> edges = new ArrayList<>();
        while (cursor.hasNext()) {
            edges.add(ArangoCpED.from(cursor.next()));
        }
        try {
            cursor.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return edges;
    }

    public List<List<String>> updateFrom(String new_id, String old_id) {
        ArangoDatabase db = client.db(dbName);
        String aql = String.format(
                "FOR e in %s FILTER e._from == '%s' UPDATE e._key WITH { _from: '%s' } IN %s " +
                        "RETURN [ OLD._from, OLD._to ]",
                edgeCollName, old_id, new_id, edgeCollName
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<List<String>> olds = new ArrayList<>();
        while (cursor.hasNext()) {
            olds.add(JSON.parseObject(cursor.next(), ArrayList.class));
        }
        try {
            cursor.close();
        } catch (Exception e) {

        }
        return olds;
    }

    /**
     *
     * @param oldFroms keys of from vertices
     * @return
     */
    public List<BaseEdgeDocument> updateFrom(String newFrom, List<String> oldFroms) {
        ArangoDatabase db = client.db(dbName);
        String from_condition = "e._from == '"+oldFroms.get(0)+"'";
        for (int i = 1; i < oldFroms.size(); i++) {
            from_condition += " OR e._from == '"+oldFroms.get(i)+"'";
        }
//        String aql = String.format(
//                "FOR e in %s FILTER %s RETURN e",
//                edgeCollName, from_condition
//        );

        String aql = String.format(
                "FOR e in %s FILTER %s UPDATE e._key WITH { _from: '%s' } IN %s RETURN OLD",
                edgeCollName, from_condition, newFrom
        );
        ArangoCursor<BaseEdgeDocument> cursor = db.query(aql, BaseEdgeDocument.class);
        List<BaseEdgeDocument> edges = new ArrayList<>();
        while (cursor.hasNext()) {
            edges.add(cursor.next());
        }
        try {
            cursor.close();
        } catch (Exception e) {

        }
        return edges;
    }


    public void upsert_VD(Collection<ArangoCpVD> vertices) {
        if (MiscellanyUtil.isArrayEmpty(vertices)) return;
        ArangoDatabase db = client.db(dbName);
        for (ArangoCpVD v : vertices) {
            db.query(v.upsertAql(vertexCollName), null);
        }
    }

    public void upsert_ED(List<ArangoCpED> edges) {
        if (MiscellanyUtil.isArrayEmpty(edges)) return;
        ArangoDatabase db = client.db(dbName);
        for (ArangoCpED e : edges) {
            db.query(e.upsertAql(edgeCollName), null);
        }
    }

    public List<String> shortestPath(ArangoComInput input) {
        ArangoDatabase db = client.db(dbName);
        String aql = String.format(
                "FOR v, e IN ANY SHORTEST_PATH '%s' TO '%s' GRAPH '%s' %s RETURN v._key",
                input.getStart_id(), input.getEnd_id(), graphName
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(cursor.next());
        }
        try {
            cursor.close();
        } catch (Exception e) {

        }
        return keys;
    }

    public List<String> chain_VD(ArangoComInput input) {
        ArangoDatabase db = client.db(dbName);
        String aql = String.format(
                "FOR v, e IN %d..%d ANY '%s' GRAPH '%s' %s OPTIONS { bfs: true, uniqueVertices: 'global' } " +
                        "%s RETURN v._id ",
                input.getMinDepth(), input.getMaxDepth(), input.getStart_id(), graphName, input.getPrune(),
                input.getFilter()
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(cursor.next());
        }

        try {
            cursor.close();
        } catch (Exception e) {

        }
        return keys;
    }

    /**
     * traverse from a start vertex, and return all end vertices and their edges
     * @param start_key
     * @param minDepth
     * @param maxDepth
     * @return
     */
    public List<ArangoCpPath> traverse_VE(String start_key, int minDepth, int maxDepth) {
        if (minDepth < 1) minDepth = 1;
        if (maxDepth < minDepth) maxDepth = minDepth;
        ArangoDatabase db = client.db(dbName);

        // no WITH
        // we should use PRUNE, but it requires arangodb version > 3.4.5
//        "FOR v, e IN %d..%d ANY '%s/%s' GRAPH '%s' PRUNE e.share FILTER !e.share RETURN " +
//                "{ vertex: v, edge: e }"
        String aql = String.format(
                "FOR v, e IN %d..%d ANY '%s/%s' GRAPH '%s' OPTIONS { bfs: true, uniqueVertices: 'global' } " +
                        "FILTER !e.share RETURN { vertex: v, edge: e }",
                minDepth, maxDepth, vertexCollName, start_key, graphName
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<ArangoCpPath> paths = new ArrayList<>();
        while (cursor.hasNext()) {
            String json = cursor.next();

            paths.add(JSON.parseObject(json, ArangoCpPath.class));
        }

        try {
            cursor.close();
        } catch (Exception e) {
            // todo
        }
        return paths;
    }


}
