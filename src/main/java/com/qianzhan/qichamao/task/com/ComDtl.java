package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.BaseConfigBus;
import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

public class ComDtl extends ComBase {
    public ComDtl(String key) {
        super(key);
    }

    @Override
    public Boolean call() {
        EsCompany e_com = compack.e_com;
        MongoCompany m_com = compack.m_com;
        ArangoCpPack a_com = compack.a_com;
        String oc_code = null;
        if (e_com != null) oc_code = e_com.getOc_code();
        else if (e_com!=null) oc_code = m_com.getOc_code();
        else if (a_com!=null) oc_code = a_com.oc_code;

        Calendar calendar = Calendar.getInstance();
        calendar.set(1949, 1, 1);
        Date start = calendar.getTime();
        calendar.set(2079, 1, 1);
        Date end = calendar.getTime();

        if (oc_code != null) {
            OrgCompanyDtl dtl = MybatisClient.getCompanyDtl(oc_code);
            if (dtl != null) {
                if (e_com != null) {
                    e_com.setLegal_person(dtl.od_faRen);
                    e_com.setOc_status(ComUtil.getCompanyStatus(dtl.od_ext));
                    e_com.setRegister_money(dtl.od_regM);
                    e_com.setOc_types(ComUtil.getCompanyType(dtl.od_bussinessDes, e_com.getOc_name()));
                    if (!MiscellanyUtil.isBlank(dtl.od_regDate)) {
                        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            Date date = d.parse(dtl.od_regDate);
                            if (date.after(start) && date.before(end)) {
                                e_com.setEstablish_date(date);
                            }
                        } catch (ParseException e) {

                        }
                    }
                }
                if (m_com != null) {
                    m_com.setOc_money(dtl.od_regMoney);
                }
                if (a_com != null) {
                    int flag = NLP.recognizeName(dtl.od_faRen);

                    if (flag == 1) {
                        // try to get oc_code of this company-type legal person
//                        int pDbIndex = DbConfigBus.getDbConfig_i("redis.db.positive", 1);
                        int nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);
                        Jedis jedis = RedisClient.get(nDbIndex);
                        String codearea = jedis.get(dtl.od_faRen);
                        if (codearea == null) {
                            Set<String> codeareas = jedis.smembers("s:" + dtl.od_faRen);
                            if (MiscellanyUtil.isArrayEmpty(codeareas)) {
                                a_com.setLp(oc_code, new ArangoCpVD(dtl.od_faRen, oc_code, 1), 0);
                            } else {
                                int sn = 0;
                                for (String ca: codeareas) {
                                    String code = codearea.substring(0, 9);
                                    String area = codearea.substring(9);
                                    a_com.setLp(oc_code, new ArangoCpVD(code, dtl.od_faRen, area), sn);
                                    sn++;
                                }
                            }
                        } else {
                            String code = codearea.substring(0, 9);
                            String area = codearea.substring(9);
                            a_com.setLp(oc_code, new ArangoCpVD(code, dtl.od_faRen, area), 0);
                        }

                    } else if (flag == 2) {
                        a_com.setLp(oc_code, new ArangoCpVD(dtl.od_faRen, oc_code, 2), 0);
                    }
                }
            }

            // post handling
            if (e_com != null) {
                if (e_com.getOc_types().isEmpty()) {
                    e_com.getOc_types().add("其他");
                }
                if (e_com.getEstablish_date().before(start) || e_com.getEstablish_date().after(end)) {
                    e_com.setEstablish_date(start);
                }
            }
        }
        return true;
    }
}
