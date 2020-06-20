package com.qianzhan.qichamao.graph;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.dal.arangodb.ArangoBusinessRepository;
import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArangoBusinessPack {
    public ArangoCpPack legacyPack;

    public String oc_code;
    public String oc_area;

    public BaseDocument com;
    public Map<String, BaseDocument> lp_map = new HashMap<>();
    public Map<String, BaseEdgeDocument> r_lp_map = new HashMap<>();

    public Map<String, BaseDocument> sh_map = new HashMap<>();
    public Map<String, BaseEdgeDocument> r_sh_map = new HashMap<>();

    public Map<String, BaseDocument> sm_map = new HashMap<>();
    public Map<String, BaseEdgeDocument> r_sm_map = new HashMap<>();

    public ArangoBusinessCompany company;

    /**
     * what should to be remembered is that if a company has a relation
     * to another company, but that company has a name shared by many companies,
     * that is, we can't make sure which company truly is our wanted.
     * in this situation, we randomly choice a company instead of maintain all those companies.
      */


//    public ArangoBusinessPerson p_lp;
//    public ArangoBusinessCompany c_lp;

//    public List<ArangoBusinessPerson> p_shs;
//    public List<ArangoBusinessCompany> c_shs;
//
//    public List<ArangoBusinessPerson> p_sms;
//    public List<ArangoBusinessCompany> c_sms;
//
//    public ArangoBusinessRelation r_lp;
//    public List<ArangoBusinessRelation> r_shs;
//    public List<ArangoBusinessRelation> r_sms;

    public ArangoBusinessPack() {
//        ArangoBusinessRepository.singleton();   // preheat to load annotations
        if (GlobalConfig.getEnv() == 1) {
            legacyPack = new ArangoCpPack();
        }
    }
    public void setLp(ArangoBusinessCompany lp) {
        lp_map.put(lp.getName(), lp.to());
        String edge_key = "lp"+oc_code+lp.getKey();
        ArangoBusinessRelation r_lp = new ArangoBusinessRelation(lp.getId(),
                ArangoBusinessCompany.toId(oc_code), edge_key, 1);
        r_lp_map.put(lp.getName(), r_lp.to());
    }

    public void setLp(ArangoBusinessPerson lp) {
        lp_map.put(lp.getName(), lp.to());
        String edge_key = "lp"+lp.getKey();
        ArangoBusinessRelation r_lp = new ArangoBusinessRelation(lp.getId(),
                ArangoBusinessCompany.toId(oc_code), edge_key, 1);
        r_lp_map.put(lp.getName(), r_lp.to());
    }

    public void setMember(ArangoBusinessCompany m, String position) {
        sm_map.put(m.getName(), m.to());
        String edge_key = "sm"+oc_code+m.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(m.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 3);
        ed.setPosition(position);
        r_sm_map.put(m.getName(), ed.to());
    }

    public void setMember(ArangoBusinessPerson m, String position) {
        sm_map.put(m.getName(), m.to());
        String edge_key = "sm"+m.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(m.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 3);
        ed.setPosition(position);
        r_sm_map.put(m.getName(), ed.to());
    }

    public void setShare_holder(ArangoBusinessCompany sh, Double money, float ratio) {
        sh_map.put(sh.getName(), sh.to());
        String edge_key = "sh"+oc_code+sh.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(sh.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 2);
        ed.setMoney(money);
        ed.setRatio(ratio);
        r_sh_map.put(sh.getName(), ed.to());
    }

    public void setShare_holder(ArangoBusinessPerson sh, Double money, float ratio) {
        sh_map.put(sh.getName(), sh.to());
        String edge_key = "sh"+sh.getKey();
        ArangoBusinessRelation ed = new ArangoBusinessRelation(sh.getId(), ArangoBusinessCompany.toId(oc_code), edge_key, 2);
        ed.setMoney(money);
        ed.setRatio(ratio);
        r_sh_map.put(sh.getName(), ed.to());
    }
}
