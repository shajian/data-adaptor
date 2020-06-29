package com.qianzhan.qichamao.dal.arangodb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.arangodb.*;
import com.arangodb.entity.*;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.PersistentIndexOptions;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.RequestType;
import com.arangodb.velocystream.Response;
import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.graph.*;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * Actually, we here use ArangoDB must be concerned with Graph, instead of taking
 *  ArangoDB as a normal database.
 * @param <T>
 */
public abstract class ArangoBaseRepository<T> {
    protected ArangoDB client;
    protected Class<T> clazz;
    @Getter
    protected ArangoGraphMeta graphMeta;
    protected String database;      // database name
    protected List<ArangoCollectionMeta> collectionMetas;

    public ArangoBaseRepository() throws Exception {
        client = ArangoClient.getClient();

        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        clazz = (Class<T>) type.getActualTypeArguments()[0];

        ArangoGraphMetas graphs = clazz.getAnnotation(ArangoGraphMetas.class);
        byte env = GlobalConfig.getEnv();
        for (ArangoGraphMeta graph : graphs.value()) {
            if (graph.env() == env) {
                graphMeta = graph;
                break;
            }
        }
        if (graphMeta == null) {
            throw new Exception("can not find the ArangoGraphMeta with env="+env);
        }
        database = graphMeta.db();

        ArangoCollectionMetas collections = clazz.getAnnotation(ArangoCollectionMetas.class);
        collectionMetas = new ArrayList<>();
        for (ArangoCollectionMeta collection : collections.value()) {
            if (collection.env() == env) {
                collectionMetas.add(collection);
            }
        }
    }

    /**
     * Since collections are always come out in forms of vertex/edge collection of a graph,
     *  it does not need to create any collection individually. After graph creation, related
     *  collections are also setup.
     */
    public void createCollections() {
        ArangoDatabase db = client.db(database);
        if (!db.exists()) {
            client.createDatabase(database);
            db = client.db(database);
        }
        for (ArangoCollectionMeta meta : collectionMetas) {
            ArangoCollection collection = db.collection(meta.collection());
            if (!collection.exists()) {
                db.createCollection(meta.collection());
                collection = db.collection(meta.collection());
            }
            if (!MiscellanyUtil.isArrayEmpty(meta.indices())) {
                Collection<IndexEntity> entities = collection.getIndexes();
                Set<String> indexFields = new HashSet<>();
                for (IndexEntity entity : entities) {
                    indexFields.addAll(entity.getFields());
                }
                for (String index : meta.indices()) {
                    String[] segs = index.split(".");
                    if (indexFields.contains(segs[0])) continue;    // had been indexed
                    boolean u = false, s = false;
                    for (int i = 1; i < segs.length; ++i) {
                        if (segs[i].equals("u")) u = true;
                        else if (segs[i].equals("s")) s = true;
                    }
                    collection.ensurePersistentIndex(
                            Arrays.asList(segs[0]), new PersistentIndexOptions().unique(u).sparse(s));
                }
            }
        }
    }

    public void createGraph() throws Exception {
        ArangoDatabase db = client.db(database);
        if (!db.exists()) {
            client.createDatabase(database);
            db = client.db(database);
        }

        ArangoGraph graph = db.graph(graphMeta.graph());
        if (!graph.exists()) {
            List<EdgeDefinition> definitions = new ArrayList<>();
            EdgeDefinition definition = new EdgeDefinition().collection(graphMeta.edge())
                    .from(graphMeta.froms()).to(graphMeta.tos());
            definitions.add(definition);
            try {
                db.createGraph(graphMeta.graph(), definitions);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(String.format("%s=%s -(%s)-> %s", graphMeta.graph(),
                    String.join(",",graphMeta.froms()), graphMeta.edge(),
                        String.join(",", graphMeta.tos())));
            }
        }

        for (ArangoCollectionMeta meta : collectionMetas) {
            ArangoCollection collection = db.collection(meta.collection());
            if (!collection.exists()) continue; // if collection is alphan, skip it
            if (!MiscellanyUtil.isArrayEmpty(meta.indices())) {
                Collection<IndexEntity> entities = collection.getIndexes();
                Set<String> indexFields = new HashSet<>();
                for (IndexEntity entity : entities) {
                    indexFields.addAll(entity.getFields());
                }
                for (String index : meta.indices()) {
                    String[] segs = index.split("\\.");
                    if (indexFields.contains(segs[0])) continue;    // had been indexed
                    boolean u = false, s = false;
                    for (int i = 1; i < segs.length; ++i) {
                        if (segs[i].equals("u")) u = true;
                        else if (segs[i].equals("s")) s = true;
                    }
                    collection.ensurePersistentIndex(
                            Arrays.asList(segs[0]), new PersistentIndexOptions().unique(u).sparse(s));
                }
            }
        }
    }

    public void truncateCollection(String coll) {
        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) {
            collection.truncate();
        }
    }
    public void dropCollection(String coll) {
        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) {
            collection.drop();
        }
    }
    public void dropGraph(String coll) {
        ArangoDatabase db = client.db(database);
        ArangoGraph graph = db.graph(graphMeta.graph());
        if (graph.exists()) {
            graph.drop();
        }
    }

    /**
     * get index name by related fields.
     * @param coll
     * @param field field on which the destination index was established.
     *              For simplicity, we do not consider compound index (establish on multiply fields).
     * @return index's name
     */
    public String indexName(String coll, String field) {
        if (MiscellanyUtil.isBlank(field) || MiscellanyUtil.isBlank(coll)) return null;
        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        Collection<IndexEntity> entities = collection.getIndexes();
        for (IndexEntity entity : entities) {
            Collection<String> fields = entity.getFields();
            if (fields.size() == 1 && fields.contains(field)) {
                return entity.getName();
            }
        }
        return null;
    }

    /**
     *
     * @param coll collection name
     * @param index_identifier name of index. can be gotten by IndexEntity.getName().
     */
    public void deleteIndex(String coll, String index_identifier) {
        if (MiscellanyUtil.isBlank(coll) || MiscellanyUtil.isBlank(index_identifier)) return;

        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        collection.deleteIndex(index_identifier);
    }

    /**
     *
     * @param index_handle id of index, which is composed with coll+"/"+index_name.
     *                     can be gotten by IndexEntity.getId().
     */
    public void deleteIndex(String index_handle) {
        if (MiscellanyUtil.isBlank(index_handle)) return;
        if (!index_handle.contains("/")) return;
        ArangoDatabase db = client.db(database);
        db.deleteIndex(index_handle);
    }

    /**
     * scan document in the whole table
     * @param coll
     * @param offset
     * @param count
     * @return
     */
    public List<BaseDocument> scan(String coll, int offset, int count) {
        ArangoDatabase db = client.db(database);
        String aql = String.format("FOR v in %s LIMIT %d, %d RETURN v", coll, offset, count);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        return cursor.asListRemaining();
    }

    /**
     * get out or in degree of a given vertex
     * @param id id of given vertex
     * @param out_in  1: out degree; 2: in degree; 3: out-in degree
     * @return
     */
    public int out_in_degree(String id, int out_in) {
        if (MiscellanyUtil.isBlank(id) || !id.contains("/")) return -1;
        if (out_in < 1) out_in = 1;
        if (out_in > 3) out_in = 3;
        String filter = null;
        if (out_in == 1) {
            filter = String.format("e._from == '%s'", id);
        } else if (out_in == 2) {
            filter = String.format("e._to == '%s'", id);
        } else {
            filter = String.format("e._from == '%s' OR e.to == '%s'", id, id);
        }
        ArangoDatabase db = client.db(database);
        String aql = String.format("FOR e in %s FILTER %s RETURN e._key", graphMeta.edge(), filter);
        ArangoCursor<String> cursor = db.query(aql, String.class);

        int count = 0;
        while (cursor.hasNext()) {
            String key = cursor.next();
            if (key == null) continue;
            count++;
        }
        return count;
    }


    public List<BaseEdgeDocument> neighbours(String id) {
        return neighbours(id, 3);
    }

    /**
     * neighbour is formed between the given vertex and to which other vertex directly connects
     * @param id given vertex's id
     * @param direction 1: outbound; 2: inbound; 3: any
     * @return all edges
     */
    public List<BaseEdgeDocument> neighbours(String id, int direction) {
        if (MiscellanyUtil.isBlank(id) || !id.contains("/")) return null;
        if (direction < 1 || direction > 3) direction = 3;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return null;
        String condition = null;
        if (direction == 1) {
            condition = String.format("e._from == '%s'", id);
        } else if (direction == 2) {
            condition = String.format("e._to == '%s'", id);
        } else {
            condition = String.format("e._from == '%s' OR e._to == '%s'", id, id);
        }
        String aql = "FOR e in %s FILTER %s RETURN e";
        ArangoCursor<BaseEdgeDocument> cursor = db.query(String.format(aql, graphMeta.edge(), condition), BaseEdgeDocument.class);
        List<BaseEdgeDocument> edges = cursor.asListRemaining();
        try {
            cursor.close();
        } catch (IOException e) {

        }

        return edges;
    }

    public List<BaseDocument> connectedGraph(String start_id, int max_depth) {
        return connectedGraph(start_id, 1, max_depth);
    }

    /**
     * get all vertices of a limited connected graph.
     * limitation here is needed, or else the connected graph will be very huge.
     * @param start_id id of the start vertex
     * @param max_depth max_depth when traversing. when max_depth==1, this function decays to be `neighbours`
     * @return
     */
    public List<BaseDocument> connectedGraph(String start_id, int min_depth, int max_depth) {
        if (MiscellanyUtil.isBlank(start_id) || !start_id.contains("/")) return null;
        if (max_depth > 3) max_depth = 3;
        if (min_depth < 0) min_depth = 0;
        if (min_depth > max_depth) min_depth = max_depth;
        ArangoDatabase db = client.db(database);
        String aql = String.format(
                "FOR v IN %d..%d ANY '%s' GRAPH '%s' OPTIONS { bfs: true, uniqueVertices: 'global' } " +
                        "RETURN v",
                min_depth, max_depth, start_id, graphMeta.graph()
        );
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        List<BaseDocument> docs = cursor.asListRemaining();
        // docs may contain null-element.

        try {
            cursor.close();
        } catch (Exception e) {

        }
        return docs;
    }


    /**
     * return all paths after traversing from a start vertex
     * @param start_id
     * @param max_depth
     * @param ret_flag  because data volumn may be huge, so `ret_flag` is used to control the data field returned.
     *                  1. vertex; 2. edge; 4. vertices; 8. edges;
     */
    public List<ArangoBusinessPath> traverse(String start_id, int min_depth, int max_depth, int ret_flag) {
        if (MiscellanyUtil.isBlank(start_id) || !start_id.contains("/")) return null;
        if (max_depth < 2) max_depth = 2;
        ArangoDatabase db = client.db(database);

        // no WITH
        // we should use PRUNE, but it requires arangodb version > 3.4.5
//        "FOR v, e IN %d..%d ANY '%s/%s' GRAPH OPTIONS { bfs: true, uniqueVertices: 'global' } " +
//        "'%s' PRUNE e.share FILTER !e.share RETURN " +
//                "{ vertex: v, edge: e }"
        List<String> pairs = new ArrayList<>();
        int w = 1;
        String pair = "vertex: v";
        for (int i = 0; i < 4; ++i) {
            if (i == 1) pair = "edge: e";
            else if (i == 2) pair = "vertices: p.vertices";
            else if (i == 3) pair = "edges: p.edges";
            if ((ret_flag & w) == 1) {
                pairs.add(pair);
            }
            w <<= 1;
        }
        String aql = String.format(
                "FOR v, e, p IN %d..%d ANY '%s' GRAPH '%s'" +
                        " RETURN { %s }",
                min_depth, max_depth, start_id, graphMeta.graph(), String.join(", ", pairs)
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<ArangoBusinessPath> paths = new ArrayList<>();

        while (cursor.hasNext()) {
            String json = cursor.next();
            if (MiscellanyUtil.isBlank(json)) continue;
            paths.add(JSON.parseObject(json, ArangoBusinessPath.class));
        }

        try {
            cursor.close();
        } catch (Exception e) {
            // todo
        }
        return paths;
    }

//    public List<BaseDocument> shortestPath(String start_id, String end_id) {
//        if (MiscellanyUtil.isBlank(start_id) || !start_id.contains("/")
//            || MiscellanyUtil.isBlank(end_id) || !end_id.contains("/")) return null;
//        ArangoDatabase db = client.db(database);
//        String aql = String.format(
//                "FOR v, e IN ANY SHORTEST_PATH '%s' TO '%s' GRAPH '%s' RETURN v",
//                start_id, end_id, graphMeta.graph()
//        );
//        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
//        List<BaseDocument> vertices = cursor.asListRemaining();
//        // vertices may contain null-element.
//
//        try {
//            cursor.close();
//        } catch (Exception e) {
//
//        }
//        return vertices;
//    }

    /**
     * merge an old vertex into a new vertex
     * `from` in parameter names means both of the two vertex have 0 in-degree
     * @param old_from_id
     * @param new_from_id
     * @param reserved whether reserve the old vertex or not.
     */
    public void merge(String old_from_id, String new_from_id, boolean reserved) {
        if (MiscellanyUtil.isBlank(old_from_id) || MiscellanyUtil.isBlank(new_from_id)
                || !old_from_id.contains("/") || !new_from_id.contains("/"))
            return;

        ArangoDatabase db = client.db(database);
        String aql = String.format(
                "FOR e in %s FILTER e._from == '%s' UPDATE e._key WITH { _from: '%s' } IN %s",
                graphMeta.edge(), old_from_id, new_from_id, graphMeta.edge()
        );
        db.query(aql, String.class);
        if (!reserved) {        // do not reserved isolated vertex, remove it!
            String[] segs = old_from_id.split("/");

            ArangoCollection collection = db.collection(segs[0]);
            collection.deleteDocument(segs[1]);
        }
    }

    public void merge(List<String> old_from_ids, String new_from_id, boolean reserved) {
        if (MiscellanyUtil.isArrayEmpty(old_from_ids) || MiscellanyUtil.isBlank(new_from_id)) return;
        ArangoDatabase db = client.db(database);
        String aql = String.format(
                "FOR e in %s FILTER e._from IN ['%s'] UPDATE e._key WITH { _from: '%s' } IN %s",
                graphMeta.edge(), String.join("', '", old_from_ids), new_from_id, graphMeta.edge()
        );
        db.query(aql, String.class);
        if (!reserved) {        // do not reserved isolated vertex, remove it!
            List<String> keys = new ArrayList<>(old_from_ids.size());
            String coll = null;
            for (String id : old_from_ids) {
                String[] segs = id.split("/");
                if (coll == null)
                    coll = segs[0];
                keys.add(segs[1]);
            }
            ArangoCollection collection = db.collection(coll);
            collection.deleteDocuments(keys);
        }
    }

    // operations on document
    public void insert_e(Collection<BaseEdgeDocument> documents) {
        insert(graphMeta.edge(), documents);
    }

    public <U extends BaseDocument> void insert(List<U> us) throws Exception {
        if (MiscellanyUtil.isArrayEmpty(us)) return;
        String id = us.get(0).getId();
        if (MiscellanyUtil.isBlank(id) || !id.contains("/")) {
            throw new Exception("document 'u' must have it `id` field been set correctly");
        }
        insert(id.split("/")[0], us);
    }
    public <U extends BaseDocument> void insert(String coll, Collection<U> us) {
        if (MiscellanyUtil.isArrayEmpty(us) || MiscellanyUtil.isBlank(coll)) return;
        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        collection.insertDocuments(us, new DocumentCreateOptions().overwrite(true));
    }
    public String insert(BaseEdgeDocument document) {
        return insert(graphMeta.edge(), document);
    }
    public <U extends BaseDocument> String insert(U u) throws Exception {
        String id = u.getId();
        if (MiscellanyUtil.isBlank(id) || !id.contains("/")) {
            throw new Exception("document 'u' must have it `id` field been set correctly");
        }
        return insert(id.split("/")[0], u);
    }
    public <U extends BaseDocument> String insert(String coll, U u) {
        if (MiscellanyUtil.isBlank(coll) || u == null) return null;
        ArangoDatabase db = client.db(database);
        ArangoCollection collection = db.collection(coll);
        String id = collection.insertDocument(u, new DocumentCreateOptions().overwrite(true)).getId();
        return id;
    }


    public void delete(String coll, Collection<String> keys) {
        if (MiscellanyUtil.isArrayEmpty(keys) || MiscellanyUtil.isBlank(coll)) return;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return;
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) return;
        collection.deleteDocuments(keys);
    }

    /**
     *
     * @param id id of vertex
     */
    public void delete(String id) {
        if (MiscellanyUtil.isBlank(id)) return;
        if (id.contains("/")) {
            String[] segs = id.split("/");
            delete(segs[0], segs[1]);
        }
    }

    public void delete_e(String key) {
        delete(graphMeta.edge(), key);
    }

    public void delete(String coll, String key) {
        if (MiscellanyUtil.isBlank(key) || MiscellanyUtil.isBlank(coll)) return;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return;
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) return;
        collection.deleteDocument(key);
    }

    public BaseDocument get(String coll, String key) {
        if (MiscellanyUtil.isBlank(coll) || MiscellanyUtil.isBlank(key)) return null;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return null;
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) return null;
        return collection.getDocument(key, BaseDocument.class);
    }

    public BaseDocument get(String id) {
        if (MiscellanyUtil.isBlank(id)) return null;
        if (id.contains("/")) {
            ArangoDatabase db = client.db(database);
            if (!db.exists()) return null;
            return db.getDocument(id, BaseDocument.class);
        }
        return null;
    }

    public Collection<BaseDocument> get(String coll, List<String> keys) {
        if (MiscellanyUtil.isBlank(coll) || MiscellanyUtil.isArrayEmpty(keys)) return null;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return null;
        ArangoCollection collection = db.collection(coll);
        if (!collection.exists()) return null;
        MultiDocumentEntity<BaseDocument> docs = collection.getDocuments(keys, BaseDocument.class);
        return docs.getDocuments();
    }

    /**
     * get an edge according to _from and _to
     * @param from
     * @param to
     * @return
     */
    public BaseEdgeDocument get_e(String from, String to) {
        if (MiscellanyUtil.isBlank(from) || MiscellanyUtil.isBlank(to)) return null;
        ArangoDatabase db = client.db(database);
        if (!db.exists()) return null;
        String aql = String.format(
                "FOR e in %s FILTER e._from == '%s' AND e._to == '%s' RETURN e",
                graphMeta.edge(), from, to
        );
        ArangoCursor<BaseEdgeDocument> cursor = db.query(aql, BaseEdgeDocument.class);
        while (cursor.hasNext()) {
            BaseEdgeDocument edge = cursor.next();
            if (edge != null) return edge;
        }
        try {
            cursor.close();
        } catch (IOException e) {

        }
        return null;
    }






    public List<BaseEdgeDocument> searchByTos(Collection<String> tos) throws Exception {
        return searchByEnds(tos, 1);
    }
    public List<BaseEdgeDocument> searchByFroms(Collection<String> froms) throws Exception {
        return searchByEnds(froms, 2);
    }

    /**
     * search for edges according to edge end points.
     * @param ends list of end id
     * @param direction  2->_from; 1->_to; 3->both from and to
     * @return
     */
    public List<BaseEdgeDocument> searchByEnds(Collection<String> ends, int direction) throws Exception {
        if (MiscellanyUtil.isArrayEmpty(ends)) return null;
        ArangoDatabase db = client.db(database);
        String ids = String.join("', '", ends);
        String filter = null;
        if (direction == 1) filter = String.format("e._to IN ['%s']", ids);
        else if (direction == 2) filter = String.format("e._from IN ['%s']", ids);
        else if (direction == 3) filter = String.format("e._from IN ['%s'] OR e._to IN ['%s']", ids, ids);
        else throw new Exception("direction must be in {1,2,3}, but get "+direction);
        String aql = String.format(
                "FOR e in %s FILTER %s RETURN e",
                graphMeta.edge(), filter, ids
        );
        ArangoCursor<BaseEdgeDocument> cursor = db.query(aql, BaseEdgeDocument.class);
        List<BaseEdgeDocument> edges = cursor.asListRemaining();
        try {
            cursor.close();
        } catch (IOException e) {

        }
        return edges;
    }

    public ArangoBusinessPath shortestPath(String from, String to) {
        if (MiscellanyUtil.isBlank(from) || MiscellanyUtil.isBlank(to)) return null;
        ArangoDatabase db = client.db(database);
        String aql = String.format(
                "FOR v, e IN ANY SHORTEST_PATH '%s' TO '%s' GRAPH '%s' RETURN { vertex: v, edge: e}",
                from, to, graphMeta.graph()
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<ArangoBusinessPath> segments = new ArrayList<>();
        ArangoBusinessPath path = new ArangoBusinessPath();
        path.vertices = new ArrayList<>();
        path.edges = new ArrayList<>();
        while (cursor.hasNext()) {
            String json = cursor.next();
            if (json == null) continue;
            ArangoBusinessPath segment = JSON.parseObject(json, ArangoBusinessPath.class);
            if (segment != null && segment.vertices != null) {
                path.vertices.add(segment.vertex);
                path.edges.add(segment.edge);
            }
        }
        try {
            cursor.close();
        } catch (Exception e) {

        }


        return path;
    }

    public List<ArangoBusinessPath> kShortestPaths(String from, String to, int k) {
        if (MiscellanyUtil.isBlank(from) || MiscellanyUtil.isBlank(to)) return null;
        if (k < 1) k = 1;
        if (k > 3) k = 3;
        ArangoDatabase db = client.db(database);
        String aql = String.format(
                "FOR p IN ANY K_SHORTEST_PATHS '%s' TO '' GRAPH '' LIMIT %d RETURN {vertices: p.vertices, edges: p.edges}",
                from, to, graphMeta.graph(), k
        );
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<ArangoBusinessPath> paths = new ArrayList<>();
        while (cursor.hasNext()) {
            String json = cursor.next();
            if (json == null) continue;
            paths.add(JSON.parseObject(json, ArangoBusinessPath.class));
        }
        return paths;
    }


    public List<BaseDocument> execute(String aql) {
        if (MiscellanyUtil.isBlank(aql)) return null;
        ArangoDatabase db = client.db(database);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        return cursor.asListRemaining();
    }

    // ============== REST API ================
    public String serverVersion() {
        JSONObject map = request4Get("/_api/version");
        return (String) map.get("version");
    }

    public String dbEngine() {
        JSONObject map = request4Get("/_api/engine");
        return (String) map.get("name");
    }

    public JSONObject request4Get(String path) {
        Request request = new Request(database, RequestType.GET, path);
        Response response = client.execute(request);
        String json = response.getBody().getAsString();
        return JSON.parseObject(json);
    }
}
