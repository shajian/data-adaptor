package com.qianzhan.qichamao.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArangoCpPack {
    public String oc_code;
    public String oc_area;
    public ArangoCpVD com;  // company itself
    public ArangoCpVD lp;   // legal person
    public List<ArangoCpVD> share_holders;  // .
    public List<ArangoCpVD> senior_members;

    public List<ArangoCpED> es = Collections.synchronizedList(new ArrayList<>());

    /**
     *
     * @param code code of the company whose legal person is `lp`
     * @param lp
     */
    public void setLp(String code, ArangoCpVD lp, int sequenceNum) {
        oc_code = code;
        this.lp = lp;
        es.add(new ArangoCpED("cp/"+lp.getKey(), "cp/"+code, "lp"+sequenceNum+code, 1));
    }

    public void setMember(String code, ArangoCpVD m, String position, int sn) {
        oc_code = code;
        if (senior_members == null) senior_members = new ArrayList<>();
        senior_members.add(m);
        ArangoCpED ed = new ArangoCpED("cp/"+m.getKey(), "cp/"+code, "sm"+sn+code, 3);
        ed.setPosition(position);
        es.add(ed);
    }

    public void setShare_holder(String code, ArangoCpVD sh, Double money, int sn) {
        oc_code = code;
        if (share_holders == null) share_holders = new ArrayList<>();
        share_holders.add(sh);
        ArangoCpED ed = new ArangoCpED("cp/"+sh.getKey(), "cp/"+code, "sh"+sn+code, 2);
        ed.setMoney(money);
        es.add(ed);
    }
}
