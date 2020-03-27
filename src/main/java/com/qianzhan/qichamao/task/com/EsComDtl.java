package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.MongoCompany;
import com.qianzhan.qichamao.entity.OrgCompanyDtl;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class EsComDtl extends EsComBase {

    @Override
    public Boolean call() {
        String oc_code = getCompany() != null ? getCompany().getOc_code() : getComstat().getOc_code();
        OrgCompanyDtl dtl = MybatisClient.getCompanyDtl(oc_code);
        MongoCompany es_c = getEs_complement();
        if (getCompany() != null) {
            EsCompany company = getCompany();
            Calendar calendar = Calendar.getInstance();
            calendar.set(1949, 1, 1);
            Date start = calendar.getTime();
            calendar.set(2079, 1, 1);
            Date end = calendar.getTime();
            if (dtl != null) {
                company.setLegal_person(dtl.od_faRen);
                company.setOc_status(EsComUtil.getCompanyStatus(dtl.od_ext));
                company.setRegister_money(dtl.od_regM);
                company.setOc_types(EsComUtil.getCompanyType(dtl.od_bussinessDes, company.getOc_name()));
                if (!MiscellanyUtil.isBlank(dtl.od_regDate)) {
                    SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        Date date = d.parse(dtl.od_regDate);
                        if (date.after(start) && date.before(end)) {
                            company.setEstablish_date(date);
                        }
                    } catch (ParseException e) {

                    }
                }

                es_c.setOc_money(dtl.od_regMoney);
            }
            if (company.getOc_types().isEmpty()) {
                company.getOc_types().add("其他");
            }
            if (company.getEstablish_date().before(start) || company.getEstablish_date().after(end)) {
                company.setEstablish_date(start);
            }

            es_c.setEstablish_date(company.getEstablish_date());
        }
        if (getComstat() != null) {

        }
        return true;
    }
}
