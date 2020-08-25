package com.qcm.task.specialtask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.entity.OrgCompanyIndustry;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.task.maintask.ComUtil;
import com.qcm.task.maintask.TaskType;

import java.util.*;

public class ComIndustryTask extends BaseTask {
    public ComIndustryTask(TaskType key) {
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
