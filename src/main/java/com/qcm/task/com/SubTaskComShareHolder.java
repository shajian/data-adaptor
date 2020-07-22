package com.qcm.task.com;

import com.qcm.config.GlobalConfig;
import com.qcm.graph.ArangoBusinessCompany;
import com.qcm.graph.ArangoBusinessPerson;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.ArangoCpVD;
import com.qcm.entity.OrgCompanyDtlGD;
import com.qcm.entity.OrgCompanyGsxtDtlGD;
import com.qcm.util.MiscellanyUtil;
import com.qcm.util.NLP;

import java.util.*;

public class SubTaskComShareHolder extends SubTaskComBase {
    public SubTaskComShareHolder(TaskType key) {
        super(key);
    }
    @Override
    public void run() {
        String oc_code = null, area = null;
        if (compack.es != null) {
            oc_code = compack.es.getOc_code();
            area = compack.es.getOc_area();
        } else if (compack.arango != null) {
            oc_code = compack.arango.oc_code;
            area = compack.arango.oc_area;
        }

        if (oc_code != null) {
            List<String> holders = new ArrayList<>();
            Map<String, Double> map = new HashMap<>();
            double total_money = 0;
            if (area.startsWith("4403")) {
                List<OrgCompanyDtlGD> gds = MybatisClient.getCompanyGDs(oc_code);
                for (OrgCompanyDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    gd.og_name = gd.og_name.trim();
                    holders.add(gd.og_name);
                    Double old = map.get(gd.og_name);
                    if (old == null) {
                        map.put(gd.og_name, gd.og_money);
                    } else {
                        map.put(gd.og_name, gd.og_money + old);
                    }
                    total_money += gd.og_money;
                }
            } else {
                List<OrgCompanyGsxtDtlGD> gds = MybatisClient.getCompanyGDsGsxt(
                        oc_code, area.substring(0, 2));
                for (OrgCompanyGsxtDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name) || gd.og_status == 4) continue;
                    gd.og_name = gd.og_name.trim();
                    holders.add(gd.og_name);
                    Double old = map.get(gd.og_name);
                    if (old == null) {
                        map.put(gd.og_name, gd.og_subscribeAccount);
                    } else {
                        map.put(gd.og_name, gd.og_subscribeAccount + old);
                    }

                    total_money += gd.og_subscribeAccount;
                }
            }
            if (compack.es != null) {
                compack.es.setShare_holders(holders);
            }

            if (compack.arango != null) {
                int dist = ComUtil.edgeLength(map.size());
                for (String key : map.keySet()) {       // key: share holder's name
                    int flag = NLP.recognizeLSM(key);
                    double money = map.get(key);
                    float ratio = (float)(total_money > 0 ? money/total_money : 0);
                    if (flag == 1) {    // company-type senior member
                        List<String> codeAreas = ComUtil.getCodeAreas(key);
                        if (codeAreas.isEmpty()) {
                            String prunedName = ComUtil.pruneCompanyName(key);
                            if (prunedName.length() < key.length()) {
                                codeAreas = ComUtil.getCodeAreas(prunedName);
                                if (codeAreas.size() > 0)
                                    key = prunedName;
                            }
                        }
                        if (codeAreas.isEmpty()) {
                            if (GlobalConfig.getEnv() == 1) {
                                compack.arango.legacyPack.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 1), money, ratio, dist, false);
                            } else {
                                compack.arango.setShare_holder(new ArangoBusinessCompany(key), money, ratio);
                            }
                        } else {
                            if (GlobalConfig.getEnv() == 1) {
                                boolean share = codeAreas.size() > 1;
                                for (String codeArea : codeAreas) {
                                    String code = codeArea.substring(0, 9);
                                    String oc_area = codeArea.substring(9);
                                    compack.arango.legacyPack.setShare_holder(oc_code, new ArangoCpVD(code, key, oc_area), money, ratio, dist, share);
                                }
                            } else {
                                String ca = codeAreas.get(0);
                                compack.arango.setShare_holder(new ArangoBusinessCompany(ca.substring(0,9), key, ca.substring(9)), money, ratio);
                            }
                        }

                    } else/* if (flag == 2)*/ {
                        if (GlobalConfig.getEnv() == 1) {
                            compack.arango.legacyPack.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 2), money, ratio, dist, false);
                        } else {
                            compack.arango.setShare_holder(new ArangoBusinessPerson(key, oc_code), money, ratio);
                        }
                    }
                }
            }
        }

        countDown();
    }
}
