package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;

import java.util.ArrayList;
import java.util.List;

public class ComOldName extends ComBase {
    public ComOldName(String key) {
        super(key);
    }
    @Override
    public Boolean call() {
        if (compack.e_com != null) {
            EsCompany c = compack.e_com;
            List<String> names = new ArrayList<>();
            for (String n : MybatisClient.getCompanyOldNames(c.getOc_code())) {
                if (EsCompanyWriter.filter_out(n)) continue;
                if (n.contains("无") || n.contains("变更名称")) continue;
                names.add(n);
            }
            c.setOld_names(names);
        }
        return true;
    }
}
