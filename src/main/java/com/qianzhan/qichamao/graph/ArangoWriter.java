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

import java.util.*;

public class ArangoWriter {
    public static void insert(List<ComPack> cps) throws Exception {
        if (GlobalConfig.getEnv() == 1) {
            insert_env1(cps);
        } else {
            insert_env2(cps);
        }
    }

    private static void insert_env2(List<ComPack> cps) throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        Map<String, BaseDocument> companies = new HashMap<>();
        Map<String, BaseDocument> persons = new HashMap<>();
        Map<String, BaseEdgeDocument> relations = new HashMap<>();
        for (ComPack cp : cps) {
            ArangoBusinessPack p = cp.arango;
            // checkout the company itself
            if (p.com != null && !companies.containsKey(p.com.getKey())) {
                companies.put(p.com.getKey(), p.com);
            }
            // checkout legal person of the company
            for (BaseDocument doc : p.lp_map.values()) {
                if (doc.getId().startsWith(ArangoBusinessCompany.collection))
                    companies.put(doc.getKey(), doc);
                else
                    persons.put(doc.getKey(), doc);
            }

            for (BaseEdgeDocument doc : p.r_lp_map.values()) {
                relations.put(doc.getKey(), doc);
            }
            // checkout share holders of the company
            for (BaseDocument doc : p.sh_map.values()) {
                if (doc.getId().startsWith(ArangoBusinessCompany.collection))
                    companies.put(doc.getKey(), doc);
                else
                    persons.put(doc.getKey(), doc);
            }
            for (BaseEdgeDocument doc : p.r_sh_map.values()) {// sh of the same name are already combined in sub-tasks.
                relations.put(doc.getKey(), doc);
            }
            // checkout senior members of the company
            for (BaseDocument doc : p.sm_map.values()) {
                if (doc.getId().startsWith(ArangoBusinessCompany.collection))
                    companies.put(doc.getKey(), doc);
                else
                    persons.put(doc.getKey(), doc);
            }
            for (BaseEdgeDocument doc : p.r_sm_map.values()) {// sm of the same name are already combined in sub-tasks.
                relations.put(doc.getKey(), doc);
            }
        }
//        System.out.println(String.format("company: %s, person: %s",
//                ArangoBusinessCompany.collection, ArangoBusinessPerson.collection));
//        System.out.println(String.format("companies: %d, persons: %s",
//                companies.size(), persons.size()));
        business.insert(ArangoBusinessCompany.collection, companies.values());
        business.insert(ArangoBusinessPerson.collection, persons.values());
        business.insert_e(relations.values());
    }
    private static void insert_env1(List<ComPack> cps) throws Exception {
        Map<String, ArangoCpVD> vertices = new HashMap<>(cps.size());
        Map<String, ArangoCpED> edges = new HashMap<>(cps.size());
        for (ComPack cp : cps) {
            ArangoCpPack p = cp.arango.legacyPack;
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

    /**
     * do not need to consider for env=1.
     * @param cps
     */
    public static void update(List<ComPack> cps) throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        List<ComPack> newAdds = new ArrayList<>();
        for (ComPack cp : cps) {
            ArangoBusinessPack p = cp.arango;
            List<ArangoBusinessPath> olds = business.traverse(
                    ArangoBusinessCompany.collection+"/"+p.oc_code, 0,1, 0x1|0x2);
            if (olds.size() == 0) {
                newAdds.add(cp);
                continue;
            }

            // NB: All companies themselves are responsible for updating(except inserting) company-type vertices,
            //      so, here we only update the company itself, and insert other newly added company-type vertices.

            List<ArangoBusinessPath> remove = new ArrayList<>();

            for (ArangoBusinessPath old : olds) {
                if (old.edge == null) { // company itself
                    if (p.company.equals(old.vertex))
                        p.com = null;
                } else {
                    String prefix = old.edge._key.substring(0, 2);
                    Object name = old.vertex.name;
                    if (prefix.equals("lp")) {
                        if (!p.lp_map.containsKey(name)) // old legal person no longer exist
                            remove.add(old);
                        else {
                            p.lp_map.remove(name);  // old legal person still exist. since legal person has no other
                            p.r_lp_map.remove(name);//  attribute, for the old legal person, no update is needed.
                        }
                    }
                    else if (prefix.equals("sm")) {
                        if (!p.sm_map.containsKey(name)) // old sm no longer exist
                            remove.add(old);
                        else {
                            p.sm_map.remove(name);      //
                            BaseEdgeDocument new_sm = p.r_sm_map.get(name);
                            new_sm.setFrom(old.edge._from); // update _from, since vertex merging
                        }
                    }
                    else if (prefix.equals("sh")) {
                        if (!p.sh_map.containsKey(name))
                            remove.add(old);
                        else {
                            p.sh_map.remove(name);
                            BaseEdgeDocument new_sh = p.r_sh_map.get(name);
                            new_sh.setFrom(old.edge._from);
                        }
                    }
                }
            }

            // remove those should be removed
            // we first remove the related edges, and if person-type vertices are isolated, remove them also.
            List<String> remove_Edges = new ArrayList<>();
            List<String> remove_Vertices = new ArrayList<>();
            for (ArangoBusinessPath path : remove) {
                remove_Edges.add(path.edge._key);
                remove_Vertices.add(path.vertex._id);
            }
            business.delete(ArangoBusinessRelation.collection, remove_Edges);
            if (remove_Vertices.size() > 0) {
                List<BaseEdgeDocument> edges = business.searchByFroms(remove_Vertices);
                Set<String> nonIsolated = new HashSet<>();
                for (BaseEdgeDocument edge : edges)
                    nonIsolated.add(edge.getFrom());
                List<String> companyTyped = new ArrayList<>();
                List<String> personTyped = new ArrayList<>();
                for (String id : remove_Vertices) {
                    if (nonIsolated.contains(id)) continue;
                    String[] segs = id.split("/");
                    if (ArangoBusinessCompany.collection.equals(segs[0])) companyTyped.add(segs[1]);
                    else personTyped.add(segs[1]);
                }
                business.delete(ArangoBusinessCompany.collection, companyTyped);
                business.delete(ArangoBusinessPerson.collection, personTyped);
            }
            // upsert new
            List<BaseDocument> insert_Vertices = new ArrayList<>();
            List<BaseEdgeDocument> insert_Edges = new ArrayList<>();
            if (p.com != null) insert_Vertices.add(p.com);
            insert_Vertices.addAll(p.lp_map.values());
            insert_Edges.addAll(p.r_lp_map.values());
            insert_Vertices.addAll(p.sh_map.values());
            insert_Edges.addAll(p.r_sh_map.values());
            insert_Vertices.addAll(p.sm_map.values());
            insert_Edges.addAll(p.r_sm_map.values());

            business.insert(insert_Vertices);
            business.insert(insert_Edges);
        }
    }

}
