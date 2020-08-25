package com.qcm.graph;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.dal.arangodb.ArangoBusinessRepository;
import com.qcm.task.specialtask.ComDtlTask;
import com.qcm.util.MiscellanyUtil;

import java.util.*;

public class ArangoBusinessCompanyDiff {
    public String code;
    public ArangoBusinessCompanyLPInfo newLP;

    // name -> info
    public Map<String, ArangoBusinessCompanySMInfo> newSMs;
    public Map<String, ArangoBusinessCompanySHInfo> newSHs;

    public ArangoBusinessCompanyLPInfo oldLP;
    // name -> info
    public Map<String, ArangoBusinessCompanySMInfo> oldSMs = new HashMap<>();
    public Map<String, ArangoBusinessCompanySHInfo> oldSHs = new HashMap<>();

    // 1st: vertices that will be removed(where the connected edges will be removed automatically by arangodb)
    //      all these vertices had a degree 1 and now are not connected by this company. However, only vertices that
    //      has a key of length > 9 will be removed truly.
    // 2nd: edges that will be removed
    //      vertices that not only be connected by this company but also other companies, or have a key of length = 9,
    //      will not be removed truly, instead edges between these vertices and this company will be removed.
    // 3rd: edges that will be updated
    //      vertices that still are connected by this company, but edge properties should be changed.
    // 4th: vertices that will be inserted, after this process, build all relation edges.
    //      new LP/SH/SM of this company
    // 5th: combine person vertices, update related vertices' degrees

    // id/key of vertices to be removed
    public Set<String> removedVertices = new HashSet<>();
    // id/key of edges to be removed
    public Set<String> removedEdges = new HashSet<>();
    // stores edges after updated
    public List<BaseEdgeDocument> updatedEdges = new ArrayList<>();
    // if no new LP want to be inserted, this field is null, otherwise specifies the name of new LP
    public Map<String, BaseDocument> insertedVertices = new HashMap<>();
    public Set<BaseEdgeDocument> insertedEdges = new HashSet<>();

    private Map<String, Integer> degreeCache = new HashMap<>();
    /**
     * how to handle old lp
     * how to handle new lp
     */
    private void diffLP() throws Exception {
        String insertedLP = null;
        if (newLP != null && oldLP != null) {
            if (MiscellanyUtil.equals(newLP.name, oldLP.name)) return;      // LP should not be changed
            // not equal and not both are blank
            // delete old LP
            Integer oldLPdegree = degreeCache.get(oldLP.v_id);
            if (oldLPdegree == null) {
                oldLPdegree = ArangoBusinessRepository.singleton().degree(oldLP.v_id);
            }
            if (newSHs != null && newSHs.containsKey(oldLP.name)
                    || newSMs != null && newSMs.containsKey(oldLP.name)
                    || ArangoBusinessCompany.isUsefulCompany(oldLP.v_id)    // old LP is a company with code, keep it
                    || oldLPdegree > 1                                      // vertex has other connections
            ) {  // keep vertex, delete edge
                // delete relation edge only instead of vertex itself
                removedEdges.add(oldLP.e_id.split("/")[1]);
            } else {                                                        // delete vertex(and edge also)
                removedVertices.add(oldLP.v_id);        // really delete this vertex?
            }
            insertedLP = newLP.name;
        } else if (newLP != null) {     // insert new LP
            insertedLP = newLP.name;
        }
        // if newLP == null, it means LP has not been changed

        if (!MiscellanyUtil.isBlank(insertedLP)) {
            ArangoBusinessCompanySHInfo oldSH = oldSHs.get(insertedLP);
            if (oldSH != null) {
                insertedEdges.add(new ArangoBusinessRelation(oldSH.v_id, ArangoBusinessCompany.toId(code), 1).to());
                return;
            }
            ArangoBusinessCompanySMInfo oldSM =oldSMs.get(insertedLP);
            if (oldSM != null) {
                insertedEdges.add(new ArangoBusinessRelation(oldSM.v_id, ArangoBusinessCompany.toId(code), 1).to());
                return;
            }
            //
            BaseDocument lp = ComDtlTask.fromLegalPerson(code, insertedLP);
            insertedVertices.put(insertedLP, lp);
            insertedEdges.add(new ArangoBusinessRelation(lp.getId(), ArangoBusinessCompany.toId(code), 1).to());
        }
    }

    private void diffSH() throws Exception {
        if (newSHs != null) {
            String newLPName = newLP == null ? null : newLP.name;
            for (String n : oldSHs.keySet()) {
                ArangoBusinessCompanySHInfo oldSH = oldSHs.get(n);
                Integer oldSHdegree = degreeCache.get(oldSH.v_id);
                if (oldSHdegree == null) {
                    oldSHdegree = ArangoBusinessRepository.singleton().degree(oldSH.v_id);
                }
                if (newSHs.containsKey(n)) {    // vertex itself will be kept
                    // the same, edge properties keeps the same
                    if (newSHs.get(n).equals(oldSH)) return;
                    // it's needed to update this SH('s edge)
                    updatedEdges.add(oldSH.toEdge(ArangoBusinessCompany.toId(code)));
                    // no need to do with SH vertex
                } else if (n.equals(newLPName)
                        || newSMs != null && newSMs.containsKey(n)
                        || ArangoBusinessCompany.isUsefulCompany(oldSH.v_id)
                        || oldSHdegree > 1
                ) {   // old SH will be kept and converted as new LP/SM
                    // delete old relation edge
                    removedEdges.add(oldSH.e_id.split("/")[1]);
                    // add a LP/SM relation edge, this step has been handled in diffLP/diffSM
                } else {
                    removedVertices.add(oldSH.v_id);
                }
            }
            for (String n : newSHs.keySet()) {
                if (oldSHs.containsKey(n)) continue;
                // newly added SH
                ArangoBusinessCompanySHInfo newSH = newSHs.get(n);
                ArangoBusinessRelation r = null;
                if (oldLP != null && newSH.name.equals(oldLP.name)) {
                    // add new SH edge
                    r = new ArangoBusinessRelation(oldLP.v_id, ArangoBusinessCompany.toId(code), 2);
                } else if (oldSMs.containsKey(newSH.name)) {
                    r = new ArangoBusinessRelation(oldSMs.get(newSH.name).v_id, ArangoBusinessCompany.toId(code), 2);
                } else {    // add a SH vertex
                    BaseDocument v = insertedVertices.get(newSH.name);
                    if (v == null) {
                        v = ComDtlTask.fromLegalPerson(code, newSH.name);
                        insertedVertices.put(newSH.name, v);
                    }
                    r = new ArangoBusinessRelation(v.getId(), ArangoBusinessCompany.toId(code), 2);
                }
                r.setMoney(newSH.money);
                r.setRatio(newSH.ratio);
                insertedEdges.add(r.to());
            }
        }
        // if newSHs == null, it means SH has not been changed
    }

    private void diffSM() throws Exception {
        if (newSMs != null) {
            String newLPName = newLP == null ? null : newLP.name;
            for (String n : oldSMs.keySet()) {
                ArangoBusinessCompanySMInfo oldSM = oldSMs.get(n);
                Integer oldSMDegree = degreeCache.get(oldSM.v_id);
                if (oldSMDegree == null) {
                    oldSMDegree = ArangoBusinessRepository.singleton().degree(oldSM.v_id);
                }
                if (newSMs.containsKey(n)) {    // vertex itself will be kept
                    // the same, edge properties keeps the same
                    if (newSMs.get(n).equals(oldSM)) return;
                    // it's needed to update this SH('s edge)
                    updatedEdges.add(oldSM.toEdge(ArangoBusinessCompany.toId(code)));
                    // no need to do with SH vertex
                } else if (n.equals(newLPName)
                        || newSHs != null && newSHs.containsKey(n)
                        || ArangoBusinessCompany.isUsefulCompany(oldSM.v_id)
                        || oldSMDegree > 1
                ) {   // old SM will be kept and converted as new LP/SH
                    // delete old relation edge
                    removedEdges.add(oldSM.e_id.split("/")[1]);
                    // add a LP/SH relation edge, this step has been handled in diffLP/diffSH
                } else {
                    removedVertices.add(oldSM.v_id);
                }
            }
            for (String n : newSMs.keySet()) {
                if (oldSMs.containsKey(n)) continue;
                // newly added SH
                ArangoBusinessCompanySMInfo newSM = newSMs.get(n);
                ArangoBusinessRelation r = null;
                if (oldLP != null && newSM.name.equals(oldLP.name)) {
                    // add new SM edge
                    r = new ArangoBusinessRelation(oldLP.v_id, ArangoBusinessCompany.toId(code), 3);
                } else if (oldSHs.containsKey(newSM.name)) {
                    r = new ArangoBusinessRelation(oldSHs.get(newSM.name).v_id, ArangoBusinessCompany.toId(code), 3);
                } else {    // add a SM vertex
                    BaseDocument v = insertedVertices.get(newSM.name);
                    if (v == null) {
                        v = ComDtlTask.fromLegalPerson(code, newSM.name);
                        insertedVertices.put(newSM.name, v);
                    }
                    r = new ArangoBusinessRelation(v.getId(), ArangoBusinessCompany.toId(code), 3);
                }
                r.setPosition(String.join(",", newSM.occupations));
                insertedEdges.add(r.to());
            }
        }
        // if newSMs == null, it means SM has not been changed
    }

    public void diff() throws Exception {
        Set<String> oldIds = new HashSet<>();
        if (oldLP != null) oldIds.add(oldLP.v_id);
        if (oldSHs != null) {
            for (String n : oldSHs.keySet()) {
                oldIds.add(oldSHs.get(n).v_id);
            }
        }
        if (oldSMs != null) {
            for (String n : oldSMs.keySet()) {
                oldIds.add(oldSMs.get(n).v_id);
            }
        }
        degreeCache = ArangoBusinessRepository.singleton().degrees(oldIds);

        diffLP();
        diffSH();
        diffSM();
    }

    /**
     *
     * @param set
     * @return collection -> keys of documents in this collection
     */
    public Map<String, List<String>> groupByCollection(Set<String> set) {
        Map<String, List<String>> groups = new HashMap<>();
        for (String key : set) {
            String[] segs = key.split("/");
            List<String> l = groups.get(segs[0]);
            if (l == null) {
                l = new ArrayList<>();
                groups.put(segs[0], l);
            }
            l.add(segs[1]);
        }
        return groups;
    }
}
