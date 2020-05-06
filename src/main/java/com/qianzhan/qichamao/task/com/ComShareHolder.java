package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.util.*;
import redis.clients.jedis.Jedis;

import java.util.*;

public class ComShareHolder extends ComBase {
    public ComShareHolder(String key) {
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
            List<String> holders = new ArrayList<>();
            Map<String, Double> map = new HashMap<>();
            double total_money = 0;
            if (area.startsWith("4403")) {
                List<OrgCompanyDtlGD> gds = MybatisClient.getCompanyGDs(oc_code);
                for (OrgCompanyDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    gd.og_name = gd.og_name.trim();
                    holders.add(gd.og_name);
                    map.put(gd.og_name, gd.og_money);
                    total_money += gd.og_money;
                }
            } else {
                List<OrgCompanyGsxtDtlGD> gds = MybatisClient.getCompanyGDsGsxt(
                        oc_code, area.substring(0, 2));
                for (OrgCompanyGsxtDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name) || gd.og_status == 4) continue;
                    gd.og_name = gd.og_name.trim();
                    holders.add(gd.og_name);
                    map.put(gd.og_name, gd.og_subscribeAccount);
                    total_money += gd.og_subscribeAccount;
                }
            }
            if (compack.e_com != null) {
                compack.e_com.setShare_holders(holders);
            }

            if (compack.a_com != null) {
                int dist = ComUtil.edgeLength(map.size());
                for (String key : map.keySet()) {       // key: share holder's name
                    int flag = NLP.recognizeName(key);
                    double money = map.get(key);
                    float ratio = (float)(total_money > 0 ? money/total_money : 0);
                    if (flag == 1) {    // company-type senior member
                        List<String> codeAreas = ComUtil.getCodeAreas(key);
                        if (codeAreas.isEmpty()) {
                            compack.a_com.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 1), money, ratio, dist, false);
                        } else {

//                            // store many companies sharing the same name into mongodb
//                            if (codeAreas.size() > 1) {
//                                MongoComShareName sn = new MongoComShareName();
//                                sn.name = key;
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
                                compack.a_com.setShare_holder(oc_code, new ArangoCpVD(code, key, oc_area), money, ratio, dist, share);
                            }
                        }

                    } else if (flag == 2) {
                        compack.a_com.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 2), money, ratio, dist,false);
                    }
                }
            }
        }

        countDown();
    }
}
