package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyDtlMgr;
import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ComMember extends ComBase {
    public ComMember(String key) {
        super(key);
    }
    @Override
    public void run() {
        String oc_code = null, area = null;
        if (compack.e_com != null) {
            oc_code = compack.e_com.getOc_code();
            area = compack.e_com.getOc_area();
        } else if (compack.a_com != null) {
            oc_code = compack.a_com.oc_code;
            area = compack.a_com.oc_area;
        }
        if (oc_code != null) {
            List<OrgCompanyDtlMgr> members;
            if (area.startsWith("4403")) {
                members = MybatisClient.getCompanyMembers(oc_code);
            } else {
                members = MybatisClient.getCompanyMembersGsxt(oc_code, area.substring(0, 2));
            }
            if (compack.e_com != null) {
                List<String> managers = new ArrayList<>();
                for (OrgCompanyDtlMgr member : members) {
                    if (!MiscellanyUtil.isBlank(member.om_name) && member.om_status != 4) {
                        managers.add(member.om_name);
                    }
                }
                compack.e_com.setSenior_managers(managers);
            }
            if (compack.a_com != null) {
                int sn = 0;
                for (OrgCompanyDtlMgr member : members) {
                    int flag = NLP.recognizeName(member.om_name);

                    if (flag == 1) {    // company-type senior member
//                        int nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);
//                        Jedis jedis = RedisClient.get(nDbIndex);
                        String codearea = RedisClient.get(member.om_name);
                        if (codearea == null) {
                            Set<String> codeareas = RedisClient.smembers("s:" + member.om_name);
                            if (MiscellanyUtil.isArrayEmpty(codeareas)) {
                                compack.a_com.setMember(oc_code, new ArangoCpVD(member.om_name, oc_code, 1), member.om_position, sn);
                                sn++;
                            } else {
                                for (String ca: codeareas) {
                                    String code = ca.substring(0, 9);
                                    String oc_area = ca.substring(9);
                                    compack.a_com.setMember(oc_code, new ArangoCpVD(code, member.om_name, oc_area), member.om_position, sn);
                                    sn++;
                                }
                            }
                        } else {
                            String code = codearea.substring(0, 9);
                            String oc_area = codearea.substring(9);
                            compack.a_com.setMember(oc_code, new ArangoCpVD(code, member.om_name, oc_area), member.om_position, sn);
                            sn++;
                        }

                    } else if (flag == 2) {  // natural person typed senior member
                        compack.a_com.setMember(oc_code, new ArangoCpVD(member.om_name, oc_code, 2), member.om_position, sn);
                        sn++;
                    }
                }
            }
        }

        ComBase.latch.countDown();
    }
}
