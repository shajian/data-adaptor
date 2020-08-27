package com.qcm.task.maintask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.entity.OrgCompanyList;
import com.qcm.task.specialtask.*;
import com.qcm.task.stat.BrowseCount;
import com.qcm.task.stat.CompanyStatisticsInfo;
import com.qcm.es.repository.EsCompanyRepository;
import com.qcm.dal.mybatis.MybatisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * To get a company's score, two steps must be pre-done:
 * 1. CompanyStatisticsInfo histogram
 * 2. BrowseCount histogram
 *
 */
public class ESCompanyTask extends BaseTask {
    private EsCompanyRepository repository;

    private static final Logger logger = LoggerFactory.getLogger(ESCompanyTask.class);



    public ESCompanyTask() throws Exception {
        super("config/Task_Es_Company.txt");
        // initialize ES read/write components
        repository = new EsCompanyRepository();
        checkpointName = projectName()+task.name()+repository.getIndexMeta().index();
    }

    protected void state1_pre() throws Exception {
        // create ES index
        CompanyStatisticsInfo.initialize();
        BrowseCount.initialize();

        if (!repository.exists()) {
            System.out.println(String.format(
                    "ES index %s is not existed, it will be created...",
                    repository.getIndexMeta().index()));
            repository.map();
            // todo log
        } else {
            System.out.println(String.format(
                    "ES index %s is existed.",
                    repository.getIndexMeta().index()
            ));
        }

        // register hook functions
        preHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(task));

        postHooks = new ArrayList<>();
        postHooks.add(() -> SharedData.closeBatch(task));
    }

    protected boolean state1_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;    // task finishes !!!

        com.qcm.task.specialtask.BaseTask.resetLatch(task, batch*9);
        int count = 0;
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            if (!validateCode(company.oc_code)) continue;

            String area = company.oc_area;
            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
            company.oc_name = company.oc_name.trim();
            if (filter_out(company.oc_name)) continue;


            SharedData.open(task);
            ComPack cp = SharedData.get(task);
            cp.es.loadFrom(company);
            if (cp.mongo != null) {
                cp.mongo.loadFrom(company);
            }

            pool.execute(new ComDtlTask(task));
            pool.execute(new ComMemberTask(task));
            pool.execute(new ComShareHolderTask(task));

            pool.execute(new ComContactTask(task));
            pool.execute(new ComOldNameTask(task));
            pool.execute(new ComIndustryTask(task));

            pool.execute(new ComGeoTask(task));
            pool.execute(new ComBrandTask(task));
            pool.execute(new ComTagTask(task));

            SharedData.close(task);
            count += 1;
        }

        int diff = (batch-count)*9;
        while(com.qcm.task.specialtask.BaseTask.getLatch(task).getCount() != diff) {
            Thread.sleep(100);
        }

        com.qcm.task.specialtask.BaseTask.resetLatch(task, count);
        // set weight and score
        for (ComPack cp : SharedData.getBatch(task)) {
            pool.execute(new ComScoreTask(cp));
        }

        com.qcm.task.specialtask.BaseTask.getLatch(task).await();

        System.out.println("writing into ES...");

        List<EsCompanyEntity> e_coms = new ArrayList<>();
        for (ComPack cp : SharedData.getBatch(task)) {
            e_coms.add(cp.es);
        }
        repository.index(e_coms);


        return true;
    }

    public void close() {
    }
}
