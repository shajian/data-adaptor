package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyDtlGD;
import com.qianzhan.qichamao.entity.OrgCompanyGsxtDtlGD;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class EsComShareHolder extends EsComBase {

    @Override
    public Boolean call() {
        if (getCompany() != null) {
            EsCompany c = getCompany();
            String code = c.getOc_code();
            String area = c.getOc_area();
            List<String> holders = new ArrayList<>();
            if (area.startsWith("4403")) {
                List<OrgCompanyDtlGD> gds = MybatisClient.getCompanyGDs(code);
                for (OrgCompanyDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    holders.add(gd.og_name);
                }
            } else {
                List<OrgCompanyGsxtDtlGD> gds = MybatisClient.getCompanyGDsGsxt(
                        code, area.substring(0, 2));
                for (OrgCompanyGsxtDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    holders.add(gd.og_name);
                }
            }
            c.setShare_holders(holders);
        }
        if (getComstat() != null) {

        }
        return true;
    }
}
