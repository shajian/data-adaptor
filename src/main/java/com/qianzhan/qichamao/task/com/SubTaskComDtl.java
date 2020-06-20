package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.graph.ArangoBusinessCompany;
import com.qianzhan.qichamao.graph.ArangoBusinessPack;
import com.qianzhan.qichamao.graph.ArangoBusinessPerson;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.MongoComDtl;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.es.EsCompanyEntity;
import com.qianzhan.qichamao.entity.OrgCompanyDtl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class SubTaskComDtl extends SubTaskComBase {
    public SubTaskComDtl(TaskType key) {
        super(key);
    }

    @Override
    public void run() {
        EsCompanyEntity e_com = compack.es;
        MongoComDtl m_com = compack.mongo;
        ArangoBusinessPack a_com = compack.arango;
        String oc_code = null;
        if (e_com != null) oc_code = e_com.getOc_code();
        else if (e_com!=null) oc_code = m_com.get_id();
        else if (a_com!=null) oc_code = a_com.oc_code;
        else if (compack.redis != null) oc_code = compack.redis.getCode();

        Calendar calendar = Calendar.getInstance();
        calendar.set(1949, 1, 1);
        Date start = calendar.getTime();
        calendar.set(2079, 1, 1);
        Date end = calendar.getTime();

        if (oc_code != null) {
            OrgCompanyDtl dtl = MybatisClient.getCompanyDtl(oc_code);
            if (dtl != null) {
                dtl.od_faRen = dtl.od_faRen.trim();
                if (e_com != null) {    // fill es data
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
                if (m_com != null) {    // fill mongo data
                    m_com.setOc_money(dtl.od_regMoney);
                }
                if (a_com != null) {    // fill arango data
                    int flag = NLP.recognizeName(dtl.od_faRen);
//                    int sn = 0;
                    if (flag == 1) {    // company-type legal person
                        // try to get oc_code of this company-type legal person
                        List<String> codeAreas = ComUtil.getCodeAreas(dtl.od_faRen);

                        if (codeAreas.isEmpty()) {      // this company is unknown
                            String prunedName = ComUtil.pruneCompanyName(dtl.od_faRen);
                            if (prunedName.length() < dtl.od_faRen.length()) {
                                codeAreas = ComUtil.getCodeAreas(prunedName);
                                if (codeAreas.size() > 0)
                                    dtl.od_faRen = prunedName;
                            }
                        }
                        if (codeAreas.isEmpty()) {
                            if (GlobalConfig.getEnv() == 1) {
                                a_com.legacyPack.setLp(oc_code, new ArangoCpVD(dtl.od_faRen, oc_code, 1), false);
                            } else {
                                a_com.setLp(new ArangoBusinessCompany(dtl.od_faRen));
                            }
                        } else {
                            if (GlobalConfig.getEnv() == 1) {
                                boolean share = codeAreas.size() > 1;
                                for (String codeArea : codeAreas) {
                                    String code = codeArea.substring(0, 9);
                                    String area = codeArea.substring(9);
                                    // set legal person
                                    a_com.legacyPack.setLp(oc_code, new ArangoCpVD(code, dtl.od_faRen, area), share);
                                }
                            } else {
                                String codeArea = codeAreas.get(0);
                                a_com.setLp(new ArangoBusinessCompany(
                                        codeArea.substring(0,9), dtl.od_faRen, codeArea.substring(9)));
                            }
                        }

                    } else if (flag == 2) {
                        if (GlobalConfig.getEnv() == 1) {
                            a_com.legacyPack.setLp(oc_code, new ArangoCpVD(dtl.od_faRen, oc_code, 2), false);
                        } else {
                            a_com.setLp(new ArangoBusinessPerson(dtl.od_faRen, oc_code));
                        }
                    }
                }
//                if (compack.redis != null) {
//                    byte status = ComUtil.getCompanyStatus(dtl.od_ext);
//                    compack.redis.setValid(ComUtil.isCompanyStatusNormal(status));
//                }
            }

            // post handling
            if (e_com != null) {    // correct es data
                if (e_com.getOc_types().isEmpty()) {
                    e_com.getOc_types().add("其他");
                }
                if (e_com.getEstablish_date() == null || e_com.getEstablish_date().before(start)
                        || e_com.getEstablish_date().after(end)) {
                    e_com.setEstablish_date(start);
                }
            }
        }

        countDown();
    }
}
