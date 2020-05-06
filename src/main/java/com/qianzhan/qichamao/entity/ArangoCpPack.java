package com.qianzhan.qichamao.entity;

import java.util.*;

/**
 * the same thread-method (with different input params such as `oc_code`) always operate different ArangoCpPack
 * different thread-method (such as ComMember and ComShareHolder) may operator the same ArangoCpPack but
 *  the different field (such as `share_holders` and `senior_members`)
 * In a word, no synchronization is needed.
 */
public class ArangoCpPack {
    public String oc_code;
    public String oc_area;

    public ArangoCpVD com;  // company itself


    public List<ArangoCpED> contact_edges;

    // why lps is a list, since a company has only one legal person?
    //  if a company has a company-typed legal person and this
    //  legal person has a name with different oc_codes, i.e.
    //  many companies shares the same name which is the legal person
    //  of the main company.
    public List<ArangoCpVD> lps;   // legal person
    public List<ArangoCpVD> share_holders;  // .
    public List<ArangoCpVD> senior_members;
    public List<ArangoCpED> lp_edges;
    public List<ArangoCpED> sh_edges;// = Collections.synchronizedList(new ArrayList<>());
    public List<ArangoCpED> sm_edges;

    public void setContacts(Set<String> codes) {
        contact_edges = new ArrayList<>();
        for (String code : codes) {
            if (code.equals(oc_code)) continue;

            ArangoCpED e = new ArangoCpED("cp/"+oc_code, "cp/"+code, "ct"+oc_code+code, 4, false);

            contact_edges.add(e);
        }
    }

    /**
     *
     * @param code code of the company whose legal person is `lp`
     * @param lp
     */
    public void setLp(String code, ArangoCpVD lp, boolean share) {
        oc_code = code;
        if (lps == null) {
            lps = new ArrayList<>();
            lp_edges = new ArrayList<>();
        }
        lps.add(lp);
        String edge_key = lp.getKey().contains(code) ? "lp"+lp.getKey() : "lp"+code+lp.getKey();
        lp_edges.add(new ArangoCpED("cp/"+lp.getKey(), "cp/"+code, edge_key, 1, share));
    }

    public void setMember(String code, ArangoCpVD m, String position, int dist, boolean share) {
        oc_code = code;
        if (senior_members == null) {
            senior_members = new ArrayList<>();
            sm_edges = new ArrayList<>();
        }
        senior_members.add(m);
        String edge_key = m.getKey().contains(code) ? "sm"+m.getKey() : "sm"+code+m.getKey();
        ArangoCpED ed = new ArangoCpED("cp/"+m.getKey(), "cp/"+code, edge_key, 3, share);
        ed.setPosition(position);
        ed.setDist(dist);
        sm_edges.add(ed);
    }

    public void setShare_holder(String code, ArangoCpVD sh, Double money, float ratio, int dist, boolean share) {
        oc_code = code;
        if (share_holders == null) {
            share_holders = new ArrayList<>();
            sh_edges = new ArrayList<>();
        }
        share_holders.add(sh);
        String edge_key = sh.getKey().contains(code) ? "sh"+sh.getKey() : "sh"+code+sh.getKey();
        ArangoCpED ed = new ArangoCpED("cp/"+sh.getKey(), "cp/"+code, edge_key, 2, share);
        ed.setMoney(money);
        ed.setRatio(ratio);
        ed.setDist(dist);
        sh_edges.add(ed);
    }
}
