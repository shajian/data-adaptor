package com.qcm.task.specialtask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.task.maintask.TaskType;
import com.qcm.util.MiscellanyUtil;
import com.qcm.dal.mybatis.MybatisClient;

import java.util.ArrayList;
import java.util.List;

public class ComOldNameTask extends BaseTask {
    public ComOldNameTask(TaskType key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.es != null) {
            EsCompanyEntity c = compack.es;
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
