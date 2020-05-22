package com.qianzhan.qichamao.dal.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.qianzhan.qichamao.graph.ArangoGraphCpId;
import com.qianzhan.qichamao.graph.ArangoCollectionMeta;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.io.IOException;
import java.util.List;

public class ArangoInterveneRepository extends ArangoBaseRepository<ArangoGraphCpId> {
    private static ArangoInterveneRepository _singleton;

    private ArangoInterveneRepository() throws Exception {
    }

    public static ArangoInterveneRepository singleton() throws Exception {
        if (_singleton == null) {
            _singleton = new ArangoInterveneRepository();
        }
        return _singleton;
    }

    public List<BaseDocument> searchByName(String coll, String name, int offset, int count) {
        return searchByName(coll, name,true,offset, count);
    }


    public List<BaseDocument> searchByName(String coll, String name, boolean asc, int offset, int count) {
        String sort = null;
        for (ArangoCollectionMeta meta : collectionMetas) {
            if (meta.collection().equals(coll)) {   // try to get sort field
                for (String index : meta.indices()) {
                    if (index.startsWith(name)) continue;
                    sort = index.split(".")[0];     // make sure this index is not HASH-type index
                    break;
                }
            }
        }
        return searchByName(graphMeta.froms()[0], name, sort, asc, offset, count);
    }
    /**
     * search vertices filtered by attribute `name`. make sure that `name` had been indexed, or else
     *  the response will be very lazy when data amount is large.
     * @param coll
     * @param name
     * @param sort
     * @param asc
     * @param offset
     * @param count
     * @return
     */
    public List<BaseDocument> searchByName(String coll, String name, String sort, boolean asc, int offset, int count) {
        if (MiscellanyUtil.isBlank(coll) || MiscellanyUtil.isBlank(name)) return null;
        if (offset < 0) offset = 0;
        if (count < 1) count = 1;
        String direction = asc ? "ASC" : "DESC";
        String sortClause = MiscellanyUtil.isBlank(sort) ? "" : String.format("SORT v.%s %s", sort, direction);
        String aql = String.format(
                "FOR v IN %s FILTER v.name == '%s' %s LIMIT %d %d RETURN v",
                coll, name, sortClause, offset, count
        );
        ArangoDatabase db = client.db(database);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        List<BaseDocument> vertices = cursor.asListRemaining();
        try {
            cursor.close();
        } catch (IOException e) {

        }
        return vertices;
    }
}
