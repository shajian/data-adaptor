package com.qcm.task.maintask;

import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.OrgCompanyList;
import com.qcm.es.entity.EsComEntity;
import com.qcm.es.repository.EsComRepository;
import com.qcm.task.specialtask.ComDtlTask;
import com.qcm.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class ESComTask extends BaseTask {
    private EsComRepository repository;
    public ESComTask() throws Exception {
        super("config/Task_ES_Com.txt");
        repository = new EsComRepository();
        checkpointName = projectName()+task.name()+repository.getIndexMeta().index();
    }

    protected void state1_pre() throws Exception {
    }
    protected boolean state1_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;    // task finishes !!!

        List<EsComEntity> entities = new ArrayList<>();
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            if (!validateCode(company.oc_code)) continue;
            if (MiscellanyUtil.isBlank(company.oc_name)) continue;
            company.oc_name = company.oc_name.trim();

            EsComEntity entity = new EsComEntity();
            entity.code = company.oc_code;
            entity.area = company.oc_area;
            entity.name = company.oc_name;
            entities.add(entity);
        }
        return true;
    }
}
