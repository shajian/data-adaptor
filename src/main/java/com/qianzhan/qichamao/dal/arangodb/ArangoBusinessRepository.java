package com.qianzhan.qichamao.dal.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.entity.CompanyTriple;
import com.qianzhan.qichamao.graph.*;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qichamao.graph.*;

import java.util.*;

public class ArangoBusinessRepository extends ArangoBaseRepository<ArangoBusiness> {
    private static ArangoBusinessRepository _singleton;

    private ArangoBusinessRepository() throws Exception {
        ArangoBusinessCompany.collection = graphMeta.tos()[0];
        ArangoBusinessRelation.collection = graphMeta.edge();
        for (String collection : graphMeta.froms()) {
            if (collection.equals(ArangoBusinessCompany.collection)) continue;
            ArangoBusinessPerson.collection = collection;
        }
    }

    public static ArangoBusinessRepository singleton() throws Exception {
        if (_singleton == null) {
            _singleton = new ArangoBusinessRepository();
        }

        return _singleton;
    }


    public List<PersonAggregation> aggregate(String coll, String name, int offset, int count) {
        if (MiscellanyUtil.isBlank(coll) || MiscellanyUtil.isBlank(name)) return null;
        if (count > 5) count = 5;
        if (count < 1) count = 1;
        String aql = String.format(
                "FOR v in %s FILTER v.name == '%s' SORT v.degree, v.key DESC limit %d %d RETURN v",
                coll, name, offset, count
        );
        ArangoDatabase db = client.db(database);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        List<BaseDocument> vertices = cursor.asListRemaining();
        String company_aql = "FOR e in " + graphMeta.edge() + " FILTER e._from = '%s' RETURN e";
        String connect_aql = "FOR v, e IN 1..3 ANY '%s' GRAPH '"+graphMeta.graph()
                +"' OPTIONS { bfs: true, uniqueVertices: 'global' } LIMIT %d RETURN v";
        List<PersonAggregation> aggregations = new ArrayList<>();
        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;
            PersonAggregation aggregation = new PersonAggregation();
            aggregations.add(aggregation);
            ArangoCursor<BaseEdgeDocument> cur = db.query(
                    String.format(company_aql, vertex.getId()), BaseEdgeDocument.class);
            Set<String> codes = new HashSet<>();
            for (BaseEdgeDocument company : cur.asListRemaining()) {
                if (company == null) continue;
                Long type = (Long) company.getAttribute("type");
                if (type != null) {
                    long t = type;
                    if (t == 1) aggregation.lps +=1;
                    else if (t == 2) aggregation.shs +=1;
                    else if (t == 3) aggregation.sms +=1;
                }

                String[] segs = company.getTo().split("/");
                if (segs.length > 0 && segs[1].length() == 9) {
                    codes.add(segs[1]);
                }
            }
            aggregation.total = aggregation.lps + aggregation.shs + aggregation.sms;
            int limit = aggregation.total * 2;
            if (limit > 50) {
                limit = aggregation.total + 20;
            }
            cursor = db.query(String.format(connect_aql, vertex.getId(), limit), BaseDocument.class);
            aggregation.companies = new ArrayList<>();
            aggregation.persons = new ArrayList<>();
            for (BaseDocument connect : cursor.asListRemaining()) {
                if (connect == null) continue;
                if (codes.contains(connect.getKey())) {
                    CompanyTriple triple = new CompanyTriple();
                    triple.oc_code = connect.getKey();
                    triple.oc_name = (String) connect.getAttribute("name");
                    triple.oc_area = (String) connect.getAttribute("area");
                    aggregation.companies.add(triple);
                } else if (connect.getId().startsWith(coll)) {
                    // for env=1, it should use "connect.getAttribute("type")==2" to argue that this is person vertex
                    //  however, here for efficiency, we just consider env=2 where collection name of person is
                    //  different from that of company
                    aggregation.persons.add((String) connect.getAttribute("name"));
                }
            }
        }
        return aggregations;
    }
}
