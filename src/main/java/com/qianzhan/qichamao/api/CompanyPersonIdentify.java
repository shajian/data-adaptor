package com.qianzhan.qichamao.api;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.dal.arangodb.ArangoInterveneRepository;
import com.qianzhan.qichamao.util.Cryptor;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.*;

public class CompanyPersonIdentify {
    private static ArangoInterveneRepository intervene;
    static {
        try {
            intervene = ArangoInterveneRepository.singleton();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //===================================================
    /**
     * To point out the relation between code1 and code2,
     * a person name must be supplied. It can't build
     * relation between two codes directly, instead, the
     * relation only can be build up via a person vertex.
     */
    //====================================================

    /**
     * search all clusters centered at the person vertex
     * each cluster is represented by all edges' `_to` vertices and it's unique center i.e. `_from` vertex
     * in itself.
     * @param name person name
     * @param offset starts from 0, which means how many documents to be skipped.
     * @param count batch size for once searching
     * @return collection of clusters.
     */
    public static Map<String, List<String>> searchClusters(String name, int offset, int count) {
        List<String> center_ids = new ArrayList<>();
        try {
            List<BaseDocument> centers = intervene.searchByName(intervene.getGraphMeta().froms()[0], name, offset, count);

            if (centers == null) return null;
            for (BaseDocument center : centers) {
                if (center != null) center_ids.add(center.getId());
            }
        } catch (Exception e) {
            // todo log error
            return null;
        }

        List<BaseEdgeDocument> edges = intervene.searchByFroms(center_ids);
        if (edges == null) return null;
        Map<String, List<String>> groups = new HashMap<>();
        for (BaseEdgeDocument edge : edges) {
            if (edge == null) continue;
            String from = edge.getFrom();
            List<String> group = groups.get(from);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(from, group);
            }
            group.add(edge.getTo());
        }
        return groups;
    }

    /**
     * search a concrete cluster, identified by a person name and a company code
     * @param name
     * @param code
     * @return id of person vertex if found, or else null.
     */
    public static String searchCluster(String name, String code) {
        if (MiscellanyUtil.isBlank(name) || MiscellanyUtil.isBlank(code)) return null;
        List<BaseEdgeDocument> edges = intervene.neighbours(code);
        if (MiscellanyUtil.isArrayEmpty(edges)) return null;
        String md5 = Cryptor.md5(name);
        for (BaseEdgeDocument edge : edges) {
            String from = edge.getFrom();
            if (from.endsWith(md5)) {
                return from;
            }
        }
        return null;
    }
    /**
     * whether the relation between code1 and code2 is black or white
     * @param name person name
     * @param code1 code of company 1
     * @param code2 code of company 2
     * @return  0: black;
     *          1: white;
     *          gt 1: unknown relation, where
     *              2: edge between name and code1 does not exist
     *              4: edge2 between name and code2 does not exist
     *              6: both edges do not exist.
     *          -1: some error was happened
     */
    public static int blackWhite(String name, String code1, String code2) {
        if (MiscellanyUtil.isBlank(name) || MiscellanyUtil.isBlank(code1) || MiscellanyUtil.isBlank(code2))
            return -1;

        int ret = 0;
        List<BaseEdgeDocument> edges1 = intervene.neighbours(code1);
        List<BaseEdgeDocument> edges2 = intervene.neighbours(code2);
        if (MiscellanyUtil.isArrayEmpty(edges1)) {
            ret |= 2;
        }
        if (MiscellanyUtil.isArrayEmpty(edges2)) {
            ret |= 4;
        }
        if (ret > 1) return ret;

        String md5 = Cryptor.md5(name);
        String center1 = null, center2 = null;
        for (BaseEdgeDocument edge : edges1) {
            String from = edge.getFrom();
            if (from.endsWith(md5)) {
                center1 = from;
                break;
            }
        }
        for (BaseEdgeDocument edge : edges2) {
            String from = edge.getFrom();
            if (from.endsWith(md5)) {
                center2 = from;
                break;
            }
        }
        if (center1 == null) {
            ret |= 2;
        }
        if (center2 == null) {
            ret |= 4;
        }
        if (ret > 0) {
            return ret;
        }

        if (center1.equals(center2)) return 1;      // white
        return 0;
    }

    /**
     * insert company into some cluster or newly created cluster.
     * @param cluster center person-vertex id if code belongs to some cluster
     *                or else, is person name
     * @param oc_code code of company related to the person name
     * @param oc_name
     * @return id of person vertex which used as the center of the related cluster
     */
    public static String insert2Cluster(String cluster, String oc_code, String oc_name) throws Exception {
        // insert vertex of code and edge between code and cluster center
        String cluster_id = null;
        String vertexCollection = intervene.getGraphMeta().froms()[0];
        if (cluster.contains("/")) {
            if (!MiscellanyUtil.isComposedWithAscii(cluster)) {
                throw new Exception("cluster id invalid. please compose it with " +
                        "<vertex collection name>/<md5(person name)>");
            }
            cluster_id = cluster;
        } else {
            List<BaseDocument> docs = intervene.searchByName(vertexCollection, cluster, false, 0, 1);
            long new_sq = 0;
            if (!MiscellanyUtil.isArrayEmpty(docs) && docs.get(0) != null) {
                BaseDocument doc = docs.get(0);
                Long sq = (Long) doc.getAttribute("sq");
                new_sq = sq + 1;
            }
            String key = String.format("%d%s", new_sq, Cryptor.md5(cluster));
            BaseDocument cluster_center = new BaseDocument(key);
            cluster_center.addAttribute("name", cluster);
            cluster_center.addAttribute("sq", new_sq);
            cluster_id = intervene.insert(vertexCollection, cluster_center);
            if (MiscellanyUtil.isBlank(cluster_id) || !cluster_id.contains("/")) {
                throw new Exception("failed to insert a new cluster center vertex with name of " + cluster);
            }
        }
        // insert code vertex and edge
        BaseDocument codeVertex = new BaseDocument(oc_code);
        codeVertex.addAttribute("name", oc_name);
        String code_id = intervene.insert(vertexCollection, codeVertex);
        String from_key = cluster_id.split("/")[0];
        BaseEdgeDocument edge = new BaseEdgeDocument(from_key+oc_code, cluster_id, code_id);
        intervene.insert(edge);
        return cluster_id;
    }

    // As aforementioned, white pair means they are in a same cluster, while black pair means they are in different
    //  clusters, but, if want to convert from white to black, or from black to white, how to do?


    /**
     * convert relation to white
     * @param name person name
     * @param code1
     * @param code2
     * @param name1
     * @param name2
     */
    public static void toWhite(String name, String code1, String code2, String name1, String name2) throws Exception {
        if (MiscellanyUtil.isBlank(name) || MiscellanyUtil.isBlank(code1) || MiscellanyUtil.isBlank(code2))
            return;

        String vcoll = intervene.getGraphMeta().froms()[0];
        List<BaseEdgeDocument> edges1 = intervene.neighbours(vcoll + "/" + code1);
        List<BaseEdgeDocument> edges2 = intervene.neighbours(vcoll + "/" +code2);
        String md5 = Cryptor.md5(name);
        String cluster1 = null, cluster2 = null;
        String edge_key1 = null, edge_key2 = null;
        int sq1 = 0, sq2 = 0;
        if (!MiscellanyUtil.isArrayEmpty(edges1)) {
            for (BaseEdgeDocument edge : edges1) {
                if (edge == null) continue;
                String from = edge.getFrom();
                if (from.endsWith(md5)) {
                    cluster1 = from;
                    edge_key1 = edge.getKey();
                    String key = from.split("/")[1];
                    String sq = from.substring(0, key.length() - md5.length());
                    sq1 = Integer.parseInt(sq);
                    break;
                }
            }
        }
        if (!MiscellanyUtil.isArrayEmpty(edges2)) {
            for (BaseEdgeDocument edge : edges2) {
                if (edge == null) continue;
                String from = edge.getFrom();
                if (from.endsWith(md5)) {
                    cluster2 = from;
                    edge_key2 = edge.getKey();
                    String key = from.split("/")[1];
                    String sq = from.substring(0, key.length() - md5.length());
                    sq2 = Integer.parseInt(sq);
                    break;
                }
            }
        }

        if (cluster1 != null && cluster2 != null) {
            if (cluster1.equals(cluster2)) return;  // already in a same cluster
            // cluster1 != cluster2  => black relation

            String toCheckIsolated = null;
            if (sq1 > sq2) {                // keep 2, remove 1
                intervene.delete_e(edge_key1);
                BaseEdgeDocument edge = new BaseEdgeDocument(sq2+md5+code1, cluster2, code1);
                intervene.insert(edge);
                toCheckIsolated = cluster1;
            } else {
                intervene.delete_e(edge_key2);
                BaseEdgeDocument edge = new BaseEdgeDocument(sq1+md5+code2, cluster1, code2);
                intervene.insert(edge);
                toCheckIsolated = cluster2;
            }
            List<BaseEdgeDocument> remindEdges = intervene.neighbours(toCheckIsolated);
            if (MiscellanyUtil.isArrayEmpty(remindEdges)) {
                intervene.delete(toCheckIsolated);
            }
        } else if (cluster1 != null) {
            // insert code2 into cluster1
            BaseDocument codeVertex2 = new BaseDocument(code2);
            codeVertex2.addAttribute("name", name2);
            BaseEdgeDocument edge = new BaseEdgeDocument(sq1+md5+code2, cluster1, code2);
            intervene.insert(vcoll, codeVertex2);
            intervene.insert(edge);
        } else if (cluster2 != null) {
            BaseDocument codeVertex1 = new BaseDocument(code1);
            codeVertex1.addAttribute("name", name1);
            BaseEdgeDocument edge = new BaseEdgeDocument(sq2+md5+code1, cluster2, code1);
            intervene.insert(vcoll, codeVertex1);
            intervene.insert(edge);
        } else {
            throw new Exception("no edge between name and code1 or between name or code2, " +
                    "converting to white can not be done, please add white pair using " +
                    "String cluster_id = insert2Cluster(name, code1, name1); and " +
                    "insert2Cluster(cluster_id, code2, name2);");
        }

    }

    // we can not convert relation to black directly, instead, break the wrong edge(s) firstly,
    // and then add the new correct edge(s).
    public static void breakRelation(String cluster, String code) {
        if (MiscellanyUtil.isBlank(cluster) || MiscellanyUtil.isBlank(code) || !cluster.contains("/")) return;
        String edge_key = cluster.split("/")[1] + code;
        intervene.delete_e(edge_key);
    }
}
