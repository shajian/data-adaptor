package com.qianzhan.qichamao.graph;

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
        }
    }

    private static void upsert_env1(List<ComPack> cps) throws Exception {
        Map<String, ArangoCpVD> vertices = new HashMap<>(cps.size());
        Map<String, ArangoCpED> edges = new HashMap<>(cps.size());
        for (ComPack cp : cps) {
            ArangoCpPack p = cp.a_com.oldPack;
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
            cp.insert(edocs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
