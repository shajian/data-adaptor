package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;

import java.util.ArrayList;
import java.util.List;

public class ComOldName extends ComBase {
    public ComOldName(String key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.es != null) {
            EsCompany c = compack.es;
            List<String> names = new ArrayList<>();
            for (String n : MybatisClient.getCompanyOldNames(c.getOc_code())) {
                if (MiscellanyUtil.isBlank(n)) continue;
                if (n.length() < 4 || n.equals("有限公司")) continue;
                if (n.contains("无") || n.contains("变更名称")) continue;
                names.add(n);
            }
            c.setOld_names(names);
        }

        countDown();
    }
}
