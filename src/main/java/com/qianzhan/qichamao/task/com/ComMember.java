package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.util.*;
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
                int dist = ComUtil.edgeLength(members.size());
                for (OrgCompanyDtlMgr member : members) {
                    int flag = NLP.recognizeName(member.om_name);

                    if (flag == 1) {    // company-type senior member
                        List<String> codeAreas = ComUtil.getCodeAreas(member.om_name);
                        if (codeAreas.isEmpty()) {
                            compack.a_com.setMember(oc_code, new ArangoCpVD(member.om_name, oc_code, 1), member.om_position, dist,false);
                        } else {

//                            // store many companies sharing the same name into mongodb
//                            if (codeAreas.size() > 1) {
//                                MongoComShareName sn = new MongoComShareName();
//                                sn.name = member.om_name;
//                                sn.codes = codeAreas;
//                                sn._id = Cryptor.md5(sn.name);
//                                try {
//                                    MongoClientRegistry.client(MongoClientRegistry.CollName.sharename)
//                                            .insert(BeanUtil.obj2Doc(sn));
//                                } catch (Exception e) { // maybe the doc has been inserted already
//                                    e.printStackTrace();
//                                }
//                            }

                            boolean share = codeAreas.size() > 1;
                            for (String codeArea : codeAreas) {
                                String code = codeArea.substring(0, 9);
                                String oc_area = codeArea.substring(9);
                                compack.a_com.setMember(oc_code, new ArangoCpVD(code, member.om_name, oc_area), member.om_position, dist, share);
                            }
                        }

                    } else if (flag == 2) {  // natural person typed senior member
                        compack.a_com.setMember(oc_code, new ArangoCpVD(member.om_name, oc_code, 2), member.om_position, dist, false);
                    }
                }
            }
        }

        countDown();
    }
}
