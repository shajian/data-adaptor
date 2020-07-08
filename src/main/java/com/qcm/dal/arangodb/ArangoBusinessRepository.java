package com.qcm.dal.arangodb;

import com.alibaba.fastjson.JSON;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.config.GlobalConfig;
import com.qcm.dal.MemcachedRepository;
import com.qcm.entity.CompanyTriple;
import com.qcm.util.Cryptor;
import com.qcm.util.MiscellanyUtil;
import com.qcm.graph.*;

import java.io.IOException;
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
        if (ArangoBusinessPerson.collection == null)
            ArangoBusinessPerson.collection = ArangoBusinessCompany.collection;
    }

    public static ArangoBusinessRepository singleton() throws Exception {
        if (_singleton == null) {
            _singleton = new ArangoBusinessRepository();
        }

        return _singleton;
    }

    /**
     * [SEARCH FOR LIST]
     * person aggregation. each person forms an aggregation(group).
     * @param name person name
     * @param offset    offset of (persons) to skip
     * @param count     how many persons(i.e. groups) will be retrieved.
     *                  maximum of this param is 4, because squeezing the
     *                  toothpaste is very very cool!
     * @return
     */
    public List<PersonAggregation> aggregate(String name, int offset, int count) {
        // validation check
        if (MiscellanyUtil.isBlank(name)) return null;
        if (count > 4) count = 4;
        if (count < 1) count = 1;

        String mc_key = String.format("%s-%d-%d", Cryptor.md5(name), offset, count);
        try {
            Object value = MemcachedRepository.singleton().get(mc_key);
            if (value != null) return (List<PersonAggregation>) value;
        } catch (IOException e) {
            // todo log exception
        }
        // search people vertices by name
        String coll = ArangoBusinessPerson.collection;
        String aql = String.format(
                "FOR v in %s FILTER v.name == '%s' SORT v.degree DESC, v.key limit %d, %d RETURN v",
                coll, name, offset, count
        );
        ArangoDatabase db = client.db(database);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        List<BaseDocument> vertices = cursor.asListRemaining();

        List<PersonAggregation> aggregations = new ArrayList<>();
        // traverse current batch of persons
        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;
            PersonAggregation aggregation = aggregate(vertex.getId(), 3);
            if (aggregation != null) aggregations.add(aggregation);
        }

        try {
            MemcachedRepository.singleton().aset(mc_key, aggregations);
        } catch (Exception e) {
            // todo log exception
        }
        return aggregations;
    }

//    /**
//     * get a group of person aggregation. It should be noted that at most 20 companies are returned.
//     * And
//     * @param code
//     * @param name
//     * @return
//     */
//    public PersonAggregation aggregate(String code, String name) {
//        // find the person vertex id.
//        String aql = "FOR e in %s FILTER e._to = '%s' RETURN e._from";
//        ArangoDatabase db = client.db(database);
//        List<String> froms = db.query(String.format(aql, graphMeta.edge(), code), String.class).asListRemaining();
//        String person_id = null;
//        String md5 = Cryptor.md5(name);
//        for (String from : froms) {
//            if (from == null) continue;
//            if (from.endsWith(md5)) {
//                person_id = from;
//                break;
//            }
//        }
//        if (person_id == null) return null; // can not find the person with `name` when given a related company code
//
//        return aggregate(person_id, 20);
//    }

    /**
     * [SEARCH FOR SINGLE]
     * When there are too many related companies for a person, it may take a long response time to retrieve data.
     * Use this method instead to load in section.
     * After calling this method, only at most 10 related companies are returned, and if want to get more companies,
     * please use aggregate(List<String>). All company codes are returned in first calling (this method),
     * and the returned 10 companies are those top 10 after sort ascend  all company codes, so for
     * 2-nd calling (aggregate(List<String>)), you should take the second 10 codes as the parameter.
     * @param code
     * @param name
     * @return
     */
    public PersonAggregation aggregate(String code, String name) {
        String aql = "FOR e in %s FILTER e._to = '%s' RETURN e._from";
        ArangoDatabase db = client.db(database);
        List<String> froms = db.query(String.format(aql, graphMeta.edge(), code), String.class).asListRemaining();
        String person_id = null;
        String md5 = Cryptor.md5(name);
        for (String from : froms) {
            if (from == null) continue;
            if (from.endsWith(md5)) {
                person_id = from;
                break;
            }
        }
        if (person_id == null) return null; // can not find the person with `name` when given a related company code

        aql = "FOR e in %s FILTER e._from == '%s' RETURN e";
        PersonAggregation aggregation = new PersonAggregation();
        aggregation.person = person_id;
        List<String> companies = new ArrayList<>();
        for (BaseEdgeDocument edge : db.query(String.format(aql, graphMeta.edge(), person_id),
                BaseEdgeDocument.class).asListRemaining()) {
            if (edge == null) continue;

            Long type = (Long) edge.getAttribute("type");
            if (type != null) {
                long t = type;
                String company = edge.getTo().split("/")[1];
                if (t == 1) aggregation.lps.add(company);
                else if (t == 2) aggregation.shs.add(company);
                else if (t == 3) aggregation.sms.add(company);
                companies.add(company);
            }
        }

        Collections.sort(companies);        // sort to retrieve with batch
        List<String> keys = companies.subList(0, 10);
        Collection<BaseDocument> docs = get(ArangoBusinessCompany.collection, keys);
        if (docs != null) {
            for (BaseDocument doc : docs) {
                CompanyTriple triple = new CompanyTriple();
                triple.oc_name = (String) doc.getAttribute("name");
                triple.oc_area = (String) doc.getAttribute("area");
                triple.oc_code = doc.getKey();
            }
        }
        return aggregation;
    }

    /**
     *
     * @param person_id
     * @param max max number of companies to be shown in a group
     * @return
     */
    private PersonAggregation aggregate(String person_id, int max) {
        // find all neighbours of this person
        // note that we limit number to 100.
        String aql = "FOR v, e in 1..3 ANY '%s' GRAPH '%s' OPTIONS {bfs: true} LIMIT 100 RETURN {vertex: v, edge: e}";
        ArangoDatabase db = client.db(database);
        List<String> results = db.query(String.format(aql, person_id, graphMeta.graph()), String.class)
                .asListRemaining();

        PersonAggregation aggregation = new PersonAggregation();
        aggregation.person = person_id;
        Map<String, ArangoBusinessVertex> map = new HashMap<>();
        for (String result : results) {
            if (result == null) continue;

            ArangoBusinessPath path = JSON.parseObject(result, ArangoBusinessPath.class);
            if (path.edge._from.equals(person_id)) {
                if (map.size() < max && !map.containsKey(path.vertex._key)) {
                    map.put(path.vertex._key, path.vertex);
                }
                Long tp = path.edge.type;
                if (tp != null) {
                    long t = tp;
                    if (t == 1) aggregation.lps.add(path.vertex._key);
                    else if (t == 2) aggregation.shs.add(path.vertex._key);
                    else if (t == 3) aggregation.sms.add(path.vertex._key);
                }
            } else if (path.vertex._id.startsWith(ArangoBusinessPerson.collection)
                    && aggregation.persons.size() < 5) {
                if (GlobalConfig.getEnv() == 1) {
                    if (path.vertex._key.length() > 12)
                        aggregation.persons.add(path.vertex.name);
                } else {
                    aggregation.persons.add(path.vertex.name);
                }
            }
        }
        aggregation.companies = new ArrayList<>();
        if (map.values().size()<=max) {
            for (ArangoBusinessVertex v : map.values()) {
                aggregation.companies.add(new CompanyTriple(v._key, v.name, v.area));
            }
            return aggregation;
        }

        long degree = -1;
        List<ArangoBusinessVertex> a = new ArrayList<>();
        List<ArangoBusinessVertex> b = new ArrayList<>();
        List<ArangoBusinessVertex> c = new ArrayList<>(map.values());
        while (true) {
            for (ArangoBusinessVertex v : c) {
                if (degree < 0) {
                    if (v.degree == null) { degree = 0; b.add(v); }
                    else { degree = v.degree; a.add(v); }
                } else {
                    if (v.degree != null && v.degree >= degree) {
                        a.add(v);
                    } else {
                        b.add(v);
                    }
                }
            }
            if (a.size() > 0 && a.size() <= max - aggregation.companies.size()) {
                for (ArangoBusinessVertex v : a) {
                    aggregation.companies.add(new CompanyTriple(v._key, v.name, v.area));
                }
                c = b;
            } else if (a.size() > 0) {
                c = a;
            } else {
                for (int i = aggregation.companies.size(); i < max && i < b.size(); i++) {
                    ArangoBusinessVertex v = b.get(i);
                    aggregation.companies.add(new CompanyTriple(v._key, v.name, v.area));
                }
                c.clear();
            }
            if (c.isEmpty() || aggregation.companies.size() >= max) break;

            b.clear();
            a.clear();
            degree = -1;
        }
        return aggregation;
    }

    public CompanyShareHolder Controller(String code) {
        if (MiscellanyUtil.isBlank(code)) return null;
        // we set max depth to 5 with hardly coding
        String aql = null;
        if (GlobalConfig.getEnv() == 1)
            aql = String.format("FOR v, e, p in 1..5 INBOUND '%s/%s' GRAPH '%s' " +
                    "FILTER v.type == 2 AND p.edges[*].type ALL == 2 " +
                "RETURN { vertices: p.vertices, edges: p.edges }",
                    ArangoBusinessCompany.collection, code, graphMeta.graph());
        else
            aql = String.format("FOR v, e, p in 1..5 INBOUND '%s/%s' GRAPH '%s' " +
                            "FILTER IS_SAME_COLLECTION('%s', v) AND p.edges[*].type ALL == 2 " +
                    "RETURN { vertices: p.vertices, edges: p.edges }",
                    ArangoBusinessCompany.collection, code, graphMeta.graph(), ArangoBusinessPerson.collection);
        ArangoDatabase db = client.db(database);
        ArangoCursor<String> cursor = db.query(aql, String.class);
        List<ArangoBusinessPath> paths = new ArrayList<>();
        for (String json : cursor.asListRemaining()) {
            if (json == null) continue;
            paths.add(JSON.parseObject(json, ArangoBusinessPath.class));
        }
        Map<String, Float> map = new HashMap<>();
        for (ArangoBusinessPath path : paths) {
            if (MiscellanyUtil.isArrayEmpty(path.vertices) || MiscellanyUtil.isArrayEmpty(path.edges)) continue;

            int size = path.vertices.size();
            String sh = (String) path.vertices.get(size - 1).name;  // SHARE HOLDER
            if (MiscellanyUtil.isBlank(sh)) continue;

            float ratio = 1;
            for (ArangoBusinessEdge edge : path.edges) {
                Double r = edge.ratio;
                if (r != null) {
                    ratio *= r;
                }
            }
            Float old = map.get(sh);
            if (old == null) map.put(sh, ratio);
            else map.put(sh, ratio+old);
        }
        if (map.isEmpty()) return null;
        CompanyShareHolder sh = new CompanyShareHolder();
        for (String key : map.keySet()) {
            float r = map.get(key);
            if (sh.finalRatio < r) {
                sh.finalController = key;
                sh.finalRatio = r;
            }
        }
        return sh;
    }

}
