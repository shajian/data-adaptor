package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
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
            if (area.startsWith("4403")) {
                List<OrgCompanyDtlGD> gds = MybatisClient.getCompanyGDs(oc_code);
                for (OrgCompanyDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    holders.add(gd.og_name);
                    map.put(gd.og_name, gd.og_money);
                }
            } else {
                List<OrgCompanyGsxtDtlGD> gds = MybatisClient.getCompanyGDsGsxt(
                        oc_code, area.substring(0, 2));
                for (OrgCompanyGsxtDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name) || gd.og_status == 4) continue;
                    holders.add(gd.og_name);
                    map.put(gd.og_name, gd.og_subscribeAccount);
                }
            }
            if (compack.e_com != null) {
                compack.e_com.setShare_holders(holders);
            }

            if (compack.a_com != null) {
                int sn = 0;
                for (String key : map.keySet()) {
                    int flag = NLP.recognizeName(key);

                    if (flag == 1) {    // company-type senior member
//                        int nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);
                        String codearea = RedisClient.get(key);
                        if (codearea == null) {
                            Set<String> codeareas = RedisClient.smembers("s:" + key);
                            if (MiscellanyUtil.isArrayEmpty(codeareas)) {
                                compack.a_com.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 1), map.get(key), sn);
                                sn++;
                            } else {
                                for (String ca: codeareas) {
                                    String code = ca.substring(0, 9);
                                    String oc_area = ca.substring(9);
                                    compack.a_com.setShare_holder(oc_code, new ArangoCpVD(code, key, oc_area), map.get(key), sn);
                                    sn++;
                                }
                            }
                        } else {
                            String code = codearea.substring(0, 9);
                            String oc_area = codearea.substring(9);
                            compack.a_com.setShare_holder(oc_code, new ArangoCpVD(code, key, oc_area), map.get(key), sn);
                            sn++;
                        }

                    } else if (flag == 2) {
                        compack.a_com.setShare_holder(oc_code, new ArangoCpVD(key, oc_code, 2), map.get(key), sn);
                        sn++;
                    }
                }
            }
        }

        ComBase.latch.countDown();
    }
}
