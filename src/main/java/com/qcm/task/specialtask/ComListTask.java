package com.qcm.task.specialtask;

import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.OrgCompanyList;
import com.qcm.task.maintask.TaskType;

public class ComListTask extends BaseTask {
    public ComListTask(TaskType key) {
        super(key);
    }

    @Override
    public void run() {
        String code = null;
        if (compack.redis != null) code = compack.redis.getCode();
        else if (compack.arango != null) code = compack.arango.oc_code;
        if (compack.redis != null) {
            OrgCompanyList c = MybatisClient.getCompany(code);
            compack.redis.setName(c.oc_name);
            compack.redis.setArea(c.oc_area);
        }
    }
}
