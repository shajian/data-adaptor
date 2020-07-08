package com.qcm.task.com;

import com.qcm.es.EsCompanyEntity;
import com.qcm.util.MiscellanyUtil;
import com.qcm.dal.mybatis.MybatisClient;

import java.util.ArrayList;
import java.util.List;

public class SubTaskComBrand extends SubTaskComBase {

    private boolean tbl_tail;
    public SubTaskComBrand(TaskType key) {
        super(key);
    }

    @Override
    public void run() {
        String tail = "";
        try {
            tail = SharedData.getConfig(task).getBool("local") ? "_temp" : "";
        } catch (Exception e) {

        }

        if (compack.es != null) {
            EsCompanyEntity c = compack.es;
            List<String> brands = new ArrayList<>();
            for (String name : MybatisClient.getCompanyBrands(c.getOc_code(), tail)) {
                if (MiscellanyUtil.isBlank(name)) continue;
                brands.add(name);
            }
            c.setBrands(brands);
        }

        countDown();
    }
}
