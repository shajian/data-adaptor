package com.qianzhan.qichamao.graph;

import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.dal.arangodb.ArangoBusinessRepository;
import com.qianzhan.qichamao.entity.ArangoCpPack;

import java.util.ArrayList;
import java.util.List;

public class ArangoBusinessPack {
    public ArangoCpPack oldPack;

    public String oc_code;
    public String oc_area;
    public ArangoBusinessCompany company;

    /**
     * what should to be remembered is that if a company has a relation
     * to another company, but that company has a name shared by many companies,
     * that is, we can't make sure which company truly is our wanted.
     * in this situation, we randomly choice a company instead of maintain all those companies.
      */


    public ArangoBusinessPerson p_lp;
    public ArangoBusinessCompany c_lp;

    public List<ArangoBusinessPerson> p_shs;
    public List<ArangoBusinessCompany> c_shs;

    public List<ArangoBusinessPerson> p_sms;
    public List<ArangoBusinessCompany> c_sms;

    public ArangoBusinessRelation r_lp;
    public List<ArangoBusinessRelation> r_shs;
    public List<ArangoBusinessRelation> r_sms;

    public ArangoBusinessPack() throws Exception {
        ArangoBusinessRepository.singleton();   // preheat to load annotations
        if (GlobalConfig.getEnv() == 1) {
            oldPack = new ArangoCpPack();
        }
    }
    public void setLp(ArangoBusinessCompany lp) {
        c_lp = lp;
        String edge_key = "lp"+oc_code+lp.getKey();
        r_lp = new ArangoBusinessRelation(lp.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 1);
    }

    public void setLp(ArangoBusinessPerson lp) {
        p_lp = lp;
        String edge_key = "lp"+lp.getKey();
        r_lp = new ArangoBusinessRelation(lp.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 1);
    }

    public void setMember(ArangoBusinessCompany m, String position) {
        if (c_sms == null) {
            c_sms = new ArrayList<>();
        }
        if (r_sms == null) {
            r_sms = new ArrayList<>();
        }
        c_sms.add(m);
        String edge_key = "sm"+oc_code+m.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(m.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 3);
        ed.setPosition(position);
        r_sms.add(ed);
    }

    public void setMember(ArangoBusinessPerson m, String position) {
        if (p_sms == null) {
            p_sms = new ArrayList<>();
        }
        if (r_sms == null) {
            r_sms = new ArrayList<>();
        }
        p_sms.add(m);
        String edge_key = "sm"+m.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(m.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 3);
        ed.setPosition(position);
        r_sms.add(ed);
    }

    public void setShare_holder(ArangoBusinessCompany sh, Double money, float ratio) {
        if (c_shs == null) {
            c_shs = new ArrayList<>();
        }
        if (r_shs == null) {
            r_shs = new ArrayList<>();
        }
        c_shs.add(sh);
        String edge_key = "sh"+oc_code+sh.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(sh.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 2);
        ed.setMoney(money);
        ed.setRatio(ratio);
        r_shs.add(ed);
    }

    public void setShare_holder(ArangoBusinessPerson sh, Double money, float ratio) {
        if (p_shs == null) {
            p_shs = new ArrayList<>();
        }
        if (r_shs == null) {
            r_shs = new ArrayList<>();
        }
        p_shs.add(sh);
        String edge_key = "sh"+sh.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(sh.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 2);
        ed.setMoney(money);
        ed.setRatio(ratio);
        r_shs.add(ed);
    }
}
