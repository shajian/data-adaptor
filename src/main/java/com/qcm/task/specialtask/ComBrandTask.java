package com.qcm.task.specialtask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.task.maintask.SharedData;
import com.qcm.task.maintask.TaskType;
import com.qcm.util.MiscellanyUtil;
import com.qcm.dal.mybatis.MybatisClient;

import java.util.ArrayList;
import java.util.List;

public class ComBrandTask extends BaseTask {

    private boolean tbl_tail;
    public ComBrandTask(TaskType key) {
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
