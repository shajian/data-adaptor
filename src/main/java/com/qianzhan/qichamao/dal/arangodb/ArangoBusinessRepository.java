package com.qianzhan.qichamao.dal.arangodb;

import com.alibaba.fastjson.JSON;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.dal.MemcachedRepository;
import com.qianzhan.qichamao.entity.CompanyTriple;
import com.qianzhan.qichamao.graph.*;
import com.qianzhan.qichamao.util.Cryptor;
import com.qianzhan.qichamao.util.MiscellanyUtil;

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
                "FOR v in %s FILTER v.name == '%s' SORT v.degree, v.key DESC limit %d, %d RETURN v",
                coll, name, offset, count
        );
        ArangoDatabase db = client.db(database);
        ArangoCursor<BaseDocument> cursor = db.query(aql, BaseDocument.class);
        List<BaseDocument> vertices = cursor.asListRemaining();

        List<PersonAggregation> aggregations = new ArrayList<>();
        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;
            PersonAggregation aggregation = aggregate(vertex.getId(), 5);
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
     * This method just cooperates with aggregate(String code, String name).
     * @param keys
     * @return
     */
    public PersonAggregation aggregate(List<String> keys) {
        PersonAggregation aggregation = new PersonAggregation();
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

    private PersonAggregation aggregate(String person_id, int max) {
        // find all neighbours of this person
        // note that we limit number to 100.
        String aql = "FOR v, e in 1..3 OUTBOUND '%s' GRAPH '%s' LIMIT 100 RETURN {vertex: v, edge: e}";
        ArangoDatabase db = client.db(database);
        List<String> results = db.query(String.format(aql, person_id, graphMeta.graph()), String.class)
                .asListRemaining();

        PersonAggregation aggregation = new PersonAggregation();
        aggregation.person = person_id;
        for (String result : results) {
            if (result == null) continue;

            ArangoGraphPath path = JSON.parseObject(result, ArangoGraphPath.class);
            if (path.edge.getFrom().equals(person_id)) {
                if (aggregation.companies.size() < max) {
                    CompanyTriple triple = new CompanyTriple();
                    triple.oc_name = (String) path.vertex.getAttribute("name");
                    triple.oc_code = path.vertex.getKey();
                    triple.oc_area = (String) path.vertex.getAttribute("area");
                    aggregation.companies.add(triple);
                }
                Long tp = (Long) path.edge.getAttribute("type");
                if (tp != null) {
                    long t = tp;
                    if (t == 1) aggregation.lps.add(path.vertex.getKey());
                    else if (t == 2) aggregation.shs.add(path.vertex.getKey());
                    else if (t == 3) aggregation.sms.add(path.vertex.getKey());
                }
            } else if (path.vertex.getId().startsWith(ArangoBusinessPerson.collection)
                    && aggregation.persons.size() < 5) {
                if (GlobalConfig.getEnv() == 1) {
                    Long type = (Long) path.vertex.getAttribute("type");
                    if (type != null && type == 2) {
                        aggregation.persons.add((String) path.vertex.getAttribute("name"));
                    }
                } else {
                    aggregation.persons.add((String) path.vertex.getAttribute("name"));
                }
            }
        }

        return aggregation;
    }

    public void Controller(String code) {
        if (MiscellanyUtil.isBlank(code)) return;
        String aql = "FOR v, e, p in 1..3 INBOUND '%s/%s' GRAPH '%s' FILTER p.edges[*].type ALL == 2 " +
                "RETURN { vertices: p.vertices, edges: p.edges }";
        ArangoDatabase db = client.db(database);
        ArangoCursor<String> cursor = db.query(String.format(aql, ArangoBusinessCompany.collection, code, graphMeta.graph()), String.class);
        List<ArangoGraphPath> paths = new ArrayList<>();
        for (String json : cursor.asListRemaining()) {
            if (json == null) continue;
            paths.add(JSON.parseObject(json, ArangoGraphPath.class));
        }
    }
}
