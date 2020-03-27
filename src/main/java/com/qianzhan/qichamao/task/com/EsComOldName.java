package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;

import java.util.ArrayList;
import java.util.List;

public class EsComOldName extends EsComBase {
    @Override
    public Boolean call() {
        if (getCompany() != null) {
            EsCompany c = getCompany();
            List<String> names = new ArrayList<>();
            for (String n : MybatisClient.getCompanyOldNames(c.getOc_code())) {
                if (EsCompanyWriter.filter_out(n)) continue;
                if (n.contains("无") || n.contains("变更名称")) continue;
                names.add(n);
            }
            c.setOld_names(names);
        }
        if (getComstat() != null) {
            EsComStat s = getComstat();

        }
        return true;
    }
}
