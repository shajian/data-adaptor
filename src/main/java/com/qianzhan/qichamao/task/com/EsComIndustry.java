package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyIndustry;

import java.util.*;

public class EsComIndustry extends EsComBase {
    @Override
    public Boolean call() {
        if (getCompany() != null) {
            EsCompany c = getCompany();
            String code = c.getOc_code();
            List<OrgCompanyIndustry> industries = MybatisClient.getCompanyIndustries(
                    code, code.substring(5, 8));
            for (OrgCompanyIndustry industry : industries) {
                if (industry.oc_type == 10 && industry.oc_data != null) {
                    c.setGb_codes(Arrays.asList(industry.oc_data.split(";")));
                    Set<String> mains = new HashSet<>();
                    List<String> cats = new ArrayList<>();
                    for (String gb : c.getGb_codes()) {
                        if (gb.length() >= 2) {
                            String main = EsComUtil.getMainIndustry(Integer.parseInt(gb.substring(0, 2)));
                            if (main != null && !mains.contains(main)) {
                                mains.add(main);
                                cats.add(main);
                            }
                        }
                    }
                    c.setGb_cats(cats);
                }
            }
        }

        if (getComstat() != null) {
            EsComStat s = getComstat();
        }
        return true;
    }
}
