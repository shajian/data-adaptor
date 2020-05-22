package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.task.stat.BrowseCount;
import com.qianzhan.qichamao.task.stat.CompanyStatisticsInfo;
import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
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
public class EsCompanyWriter extends BaseWriter {
    private EsCompanyRepository repository;

    private static final Logger logger = LoggerFactory.getLogger(EsCompanyWriter.class);



    public EsCompanyWriter() throws Exception {
        super("config/EsCompany.txt");
        // initialize ES read/write components
        repository = new EsCompanyRepository();
        checkpointName = "data-adaptor.es."+repository.getIndexMeta().index();
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
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        postHooks = new ArrayList<>();
        for (int task : tasks) {
            if ((task & TaskType.mongo.getValue()) != 0) {
                postHooks.add(() -> MongodbCompanyWriter.writeDtl2Db(tasks_key));
            }
            if ((task & TaskType.arango.getValue()) != 0) {
                postHooks.add(() -> ArangodbCompanyWriter.upsert_static(tasks_key));
            }
        }
        postHooks.add(() -> SharedData.closeBatch(tasks_key));
    }

    protected boolean state1_inner() throws Exception{
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;    // task finishes !!!

        ComBase.resetLatch(tasks_key, batch*9);
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


            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);
            cp.e_com.loadFrom(company);
            if (cp.m_com != null) {
                cp.m_com.loadFrom(company);
            }

            pool.execute(new ComDtl(tasks_key));
            pool.execute(new ComMember(tasks_key));
            pool.execute(new ComShareHolder(tasks_key));

            pool.execute(new ComContact(tasks_key));
            pool.execute(new ComOldName(tasks_key));
            pool.execute(new ComIndustry(tasks_key));

            pool.execute(new ComGeo(tasks_key));
            pool.execute(new ComBrand(tasks_key));
            pool.execute(new ComTag(tasks_key));

            SharedData.close(tasks_key);
            count += 1;
        }

        int diff = (batch-count)*9;
        while(ComBase.getLatch(tasks_key).getCount() != diff) {
            Thread.sleep(100);
        }

        ComBase.resetLatch(tasks_key, count);
        // set weight and score
        for (ComPack cp : SharedData.getBatch(tasks_key)) {
            pool.execute(new ComScore(cp));
        }

        ComBase.getLatch(tasks_key).await();

        System.out.println("writing into ES...");

        List<EsCompany> e_coms = new ArrayList<>();
        for (ComPack cp : SharedData.getBatch(tasks_key)) {
            e_coms.add(cp.e_com);
        }
        repository.index(e_coms);


        return true;
    }

    public void close() {
    }
}
