package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.es.EsCompanyEntity;
import com.qianzhan.qichamao.entity.OrgCompanyIndustry;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;

import java.util.*;

public class SubTaskComIndustry extends SubTaskComBase {
    public SubTaskComIndustry(TaskType key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.es != null) {
            EsCompanyEntity c = compack.es;
            String code = c.getOc_code();
            try {
                List<OrgCompanyIndustry> industries = MybatisClient.getCompanyIndustries(
                        code, code.substring(5, 8));
                for (OrgCompanyIndustry industry : industries) {
                    if (industry.oc_type == 10 && industry.oc_data != null) {
                        c.setGb_codes(Arrays.asList(industry.oc_data.split(";")));
                        Set<String> mains = new HashSet<>();
                        List<String> cats = new ArrayList<>();
                        for (String gb : c.getGb_codes()) {
                            if (gb.length() >= 2) {
                                String main = ComUtil.getMainIndustry(Integer.parseInt(gb.substring(0, 2)));
                                if (main != null && !mains.contains(main)) {
                                    mains.add(main);
                                    cats.add(main);
                                }
                            }
                        }
                        c.setGb_cats(cats);
                    }
                }
            } catch (Exception e) {

            }
        }

        countDown();
    }
}
