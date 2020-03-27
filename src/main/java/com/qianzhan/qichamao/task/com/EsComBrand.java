package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.EsConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class EsComBrand extends EsComBase {

    @Override
    public Boolean call() throws Exception {
        String tail = EsConfigBus.getTaskConfigBool("local") ? "_temp" : "";
        if (getCompany() != null) {
            EsCompany c = getCompany();
            List<String> brands = new ArrayList<>();
            for (String name : MybatisClient.getCompanyBrands(c.getOc_code(), tail)) {
                if (MiscellanyUtil.isBlank(name)) continue;
                brands.add(name);
            }
            c.setBrands(brands);
        }
        return true;
    }
}
