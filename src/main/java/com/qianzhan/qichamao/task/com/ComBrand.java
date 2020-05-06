package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.EsConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class ComBrand extends ComBase {

    private boolean tbl_tail;
    public ComBrand(String key) {
        super(key);
    }

    @Override
    public void run() {
        String tail = "";
        try {
            tail = SharedData.getConfig(tasks_key).getBool("local") ? "_temp" : "";
        } catch (Exception e) {

        }

        if (compack.e_com != null) {
            EsCompany c = compack.e_com;
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
