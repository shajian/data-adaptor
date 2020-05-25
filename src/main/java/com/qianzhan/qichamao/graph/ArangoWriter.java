package com.qianzhan.qichamao.graph;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.dal.arangodb.ArangoBusinessRepository;
import com.qianzhan.qichamao.entity.ArangoCpED;
import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.task.com.ComPack;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArangoWriter {
    public static void upsert(List<ComPack> cps) throws Exception {
        if (GlobalConfig.getEnv() == 1) {
            upsert_env1(cps);
        } else {
            upsert_env2(cps);
        }
    }

    private static void upsert_env2(List<ComPack> cps) throws Exception {
        Map<String, ArangoBusinessCompany> companies = new HashMap<>();
        Map<String, ArangoBusinessPerson> persons = new HashMap<>();
        Map<String, ArangoBusinessRelation> relations = new HashMap<>();
        for (ComPack cp : cps) {
            ArangoBusinessPack p = cp.arango;
            // checkout the company itself
            if (p.company != null) {
                if (!companies.containsKey(p.company.getKey())) {
                    companies.put(p.company.getKey(), p.company);
                }
            }
            // checkout legal person of the company
            if (p.c_lp != null && !companies.containsKey(p.c_lp.getKey())) { // c_lp and p_lp is exclusively
                companies.put(p.c_lp.getKey(), p.c_lp);
            } else if (p.p_lp != null && !persons.containsKey(p.p_lp.getKey())) {
                persons.put(p.p_lp.getKey(), p.p_lp);
            }
            if (p.r_lp != null && !relations.containsKey(p.r_lp.getKey())) {
                relations.put(p.r_lp.getKey(), p.r_lp);
            }
            // checkout share holders of the company
            if (p.c_shs != null) {
                for (ArangoBusinessCompany sh : p.c_shs) {
                    if (!companies.containsKey(sh.getKey())) {
                        companies.put(sh.getKey(), sh);
                    }
                }
            }
            if (p.p_shs != null) {
                for (ArangoBusinessPerson sh : p.p_shs) {
                    if (!persons.containsKey(sh.getKey())) {
                        persons.put(sh.getKey(), sh);
                    }
                }
            }
            if (p.r_shs != null) {
                for (ArangoBusinessRelation sh : p.r_shs) {
                    if (!relations.containsKey(sh.getKey())) {
                        relations.put(sh.getKey(), sh);
                    }
                }
            }
            // checkout senior members of the company
            if (p.c_sms != null) {
                for (ArangoBusinessCompany sh : p.c_sms) {
                    if (!companies.containsKey(sh.getKey())) {
                        companies.put(sh.getKey(), sh);
                    }
                }
            }
            if (p.p_sms != null) {
                for (ArangoBusinessPerson sh : p.p_sms) {
                    if (!persons.containsKey(sh.getKey())) {
                        persons.put(sh.getKey(), sh);
                    }
                }
            }
            if (p.r_sms != null) {
                for (ArangoBusinessRelation sh : p.r_sms) {
                    if (!relations.containsKey(sh.getKey())) {
                        relations.put(sh.getKey(), sh);
                    }
                }
            }
        }
        List<BaseDocument> c_docs = new ArrayList<>();
        List<BaseDocument> p_docs = new ArrayList<>();
        List<BaseEdgeDocument> r_docs = new ArrayList<>();

        for (String key : companies.keySet()) {
            c_docs.add(companies.get(key).to());
        }
        for (String key : persons.keySet()) {
            p_docs.add(persons.get(key).to());
        }
        for (String key : relations.keySet()) {
            r_docs.add(relations.get(key).to());
        }
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        try {
            business.insert(c_docs);
        } catch (Exception e) {

        }
        try {
            business.insert(p_docs);
        } catch (Exception e) {

        }
        business.insert_e(r_docs);
    }
    private static void upsert_env1(List<ComPack> cps) throws Exception {
        Map<String, ArangoCpVD> vertices = new HashMap<>(cps.size());
        Map<String, ArangoCpED> edges = new HashMap<>(cps.size());
        for (ComPack cp : cps) {
            ArangoCpPack p = cp.arango.oldPack;
            String code = p.oc_code;
            if (MiscellanyUtil.isBlank(code)) {
                if (p.com != null) code = p.com.getKey();
                if (MiscellanyUtil.isBlank(code)) {
                    throw new ValueException("ArangoCpPack.oc_code can not be null or empty if you want to " +
                            "execute Arangodb writing task. Please set ArangoCpPack.oc_code or " +
                            "ArangoCpPack.com");
                }
            }
            // add main company vertex
            if (p.com != null) {
                if (!vertices.containsKey(code)) {
                    vertices.put(code, p.com);
                }
            }


            // add legal person vertex
            if (p.lps != null) {
                for (int i = 0; i < p.lps.size(); i++) {
                    ArangoCpVD v = p.lps.get(i);        // legal person vertex
                    ArangoCpED e = p.lp_edges.get(i);
                    // `code` is oc_code of main company
                    // legal person of main company starting with `code` means that
                    //  it is not independent.
                    if (!vertices.containsKey(v.getKey())) {
                        vertices.put(v.getKey(), v);
                    }

                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    }
                }
            }

            // add share holder vertex
            if (p.share_holders != null) {
                for (int i = 0; i < p.share_holders.size(); i++) {
                    ArangoCpVD v = p.share_holders.get(i);
                    ArangoCpED e = p.sh_edges.get(i);
                    if (!vertices.containsKey(v.getKey())) {
                        vertices.put(v.getKey(), v);
                    }

                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    } else {    // combine money and ratio of the same share holder
                        ArangoCpED old = edges.get(e.getKey());
                        old.setMoney(old.getMoney()+e.getMoney());
                        old.setRatio(old.getRatio()+e.getRatio());
                    }
                }
            }

            if (p.senior_members != null) {
                for (int i = 0; i < p.senior_members.size(); i++) {
                    ArangoCpVD v = p.senior_members.get(i);
                    ArangoCpED e = p.sm_edges.get(i);
                    if (!vertices.containsKey(v.getKey())) {
                        vertices.put(v.getKey(), v);
                    }
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    } else {        // combine positions of the same member
                        ArangoCpED old = edges.get(e.getKey());
                        old.setPosition(old.getPosition()+","+e.getPosition());
                    }
                }
            }

            if (p.contact_edges != null) {
                for (ArangoCpED e : p.contact_edges) {
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    }
                }
            }
        }

        // for dependent vertices and edges, because their key are different from each other, so bulk insert them.
        List<BaseDocument> docs = new ArrayList<>(vertices.size());
        List<BaseEdgeDocument> edocs = new ArrayList<>(edges.size());
        for (String key : vertices.keySet()) {
            docs.add(vertices.get(key).to());
        }
        for (String key : edges.keySet()) {
            edocs.add(edges.get(key).to());
        }
        ArangoBusinessRepository cp = ArangoBusinessRepository.singleton();
        try {
            cp.insert(cp.getGraphMeta().froms()[0], docs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            cp.insert_e(edocs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
