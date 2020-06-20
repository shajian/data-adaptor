package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.graph.ArangoBusinessCompany;
import com.qianzhan.qichamao.graph.ArangoBusinessPerson;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.OrgCompanyDtlMgr;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;

import java.util.*;

public class SubTaskComMember extends SubTaskComBase {
    public SubTaskComMember(TaskType key) {
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
            List<OrgCompanyDtlMgr> members;
            if (area.startsWith("4403")) {
                members = MybatisClient.getCompanyMembers(oc_code);
            } else {
                members = MybatisClient.getCompanyMembersGsxt(oc_code, area.substring(0, 2));
            }
            Map<String, OrgCompanyDtlMgr> map = new HashMap<>();
            for (OrgCompanyDtlMgr member : members) {
                if (MiscellanyUtil.isBlank(member.om_name) || member.om_status == 4) continue;
                member.om_name = member.om_name.trim();
                OrgCompanyDtlMgr old = map.get(member.om_name);
                if (old == null) {
                    map.put(member.om_name, member);
                } else {
                    old.om_position += "," + member.om_position;
                }
            }
            if (compack.es != null) {
                compack.es.setSenior_managers(new ArrayList<>(map.keySet()));
            }
            if (compack.arango != null) {
                int dist = ComUtil.edgeLength(map.size());
                for (String name : map.keySet()) {
                    String occupy = map.get(name).om_position;
                    if (!MiscellanyUtil.isBlank(occupy)) {
                        Set<String> set = new HashSet<>();
                        for (String seg : occupy.split(",")) {
                            set.add(seg);
                        }
                        occupy = String.join(",", set);
                    }
                    int flag = NLP.recognizeName(name);

                    if (flag == 1) {    // company-type senior member
                        List<String> codeAreas = ComUtil.getCodeAreas(name);
                        if (codeAreas.isEmpty()) {  // unknown company
                            //
                            String prunedName = ComUtil.pruneCompanyName(name);
                            if (prunedName.length() < name.length()) {
                                codeAreas = ComUtil.getCodeAreas(prunedName);
                                if (codeAreas.size() > 0)
                                    name = prunedName;
                            }
                        }
                        if (codeAreas.isEmpty()) {  // unknown company
                            if (GlobalConfig.getEnv() == 1) {
                                compack.arango.legacyPack.setMember(oc_code, new ArangoCpVD(name, oc_code, 1), occupy, dist, false);
                            } else {
                                compack.arango.setMember(new ArangoBusinessCompany(name), occupy);
                            }
                        } else {
                            if (GlobalConfig.getEnv() == 1) {
                                boolean share = codeAreas.size() > 1;
                                for (String codeArea : codeAreas) {
                                    String code = codeArea.substring(0, 9);
                                    String oc_area = codeArea.substring(9);
                                    compack.arango.legacyPack.setMember(oc_code, new ArangoCpVD(code, name, oc_area), occupy, dist, share);
                                }
                            } else {
                                String codeArea = codeAreas.get(0);
                                compack.arango.setMember(new ArangoBusinessCompany(codeArea.substring(0,9), name, codeArea.substring(9)), occupy);
                            }
                        }

                    } else if (flag == 2) {  // natural person typed senior member
                        if (GlobalConfig.getEnv() == 1) {
                            compack.arango.legacyPack.setMember(oc_code, new ArangoCpVD(name, oc_code, 2), occupy, dist, false);
                        } else {
                            compack.arango.setMember(new ArangoBusinessPerson(name, oc_code), occupy);
                        }
                    }
                }
            }
        }

        countDown();
    }
}
