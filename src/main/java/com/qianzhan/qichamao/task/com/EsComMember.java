package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyDtlMgr;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class EsComMember extends EsComBase {

    @Override
    public Boolean call() {
        String oc_code = null, area = null;
        if (getCompany() != null) {
            EsCompany c = getCompany();
            oc_code = c.getOc_code();
            area = c.getOc_area();
            List<OrgCompanyDtlMgr> members;
            if (area.startsWith("4403")) {
                members = MybatisClient.getCompanyMembers(oc_code);
            } else {
                members = MybatisClient.getCompanyMembersGsxt(oc_code, area.substring(0, 2));
            }

            List<String> managers = new ArrayList<>();
            for (OrgCompanyDtlMgr member : members) {
                if (!MiscellanyUtil.isBlank(member.om_name)) {
                    managers.add(member.om_name);
                }
            }
            c.setSenior_managers(managers);
        }
        if (getComstat() != null) {
            EsComStat s = getComstat();
            oc_code = s.getOc_code();
            area = s.getArea_4();
        }
        return true;
    }
}
