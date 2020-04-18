package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.task.stat.BrowseCount;
import com.qianzhan.qichamao.task.stat.CompanyStatisticsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * To get a company's score, two steps must be pre-done:
 * 1. CompanyStatisticsInfo histogram
 * 2. BrowseCount histogram
 *
 */
public class EsCompanyWriter extends BaseWriter {
    private EsCompanyRepository repository;


    CountDownLatch latch = new CountDownLatch(3);
//    private CompletionService<Boolean> threadpool;
    private ThreadPoolExecutor pool;
    private int max_threads;
    private int thread_queue_size_ratio;
    private static final Logger logger = LoggerFactory.getLogger(EsCompanyWriter.class);



    public EsCompanyWriter() throws Exception {
        super("config/EsCompany.txt");
        // initialize ES read/write components
        repository = new EsCompanyRepository();
        checkpointName = "data-adaptor.es."+repository.getIndexMeta().index();
        thread_queue_size_ratio = config.getInt("thread_queue_size_ratio", 5);
        max_threads = config.getInt("max_threads", 0);
        if (max_threads <= 0) {
            max_threads = Runtime.getRuntime().availableProcessors() * 15;
        }
        BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>(thread_queue_size_ratio*batch);
        pool = new ThreadPoolExecutor(max_threads, max_threads, 0, TimeUnit.SECONDS,
                queue, new ThreadPoolExecutor.AbortPolicy());
//        threadpool = new ExecutorCompletionService(pool);
    }

    protected void preCreate() throws Exception {
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
                postHooks.add(() -> MongodbCompanyWriter.write2Db(tasks_key));
            }
            if ((task & TaskType.arango.getValue()) != 0) {
                postHooks.add(() -> ArangodbCompanyWriter.upsert(tasks_key));
            }
        }
        postHooks.add(() -> SharedData.closeBatch(tasks_key));
    }

    protected boolean createInner() throws Exception{
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;    // task finishes !!!

        ComBase.latch = new CountDownLatch(batch*9);
        int count = 0;
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            char codeTail = company.oc_code.charAt(8);
            String area = company.oc_area;
            if (codeTail == 'T' || codeTail == 'K'
                    || area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
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
        while(ComBase.latch.getCount() != diff) {
            Thread.sleep(2000);
        }

        ComBase.latch = new CountDownLatch(count);
        // set weight and score
        for (ComPack cp : SharedData.getBatch(tasks_key)) {
            pool.execute(new ComScore(cp));
        }

        ComBase.latch.await();

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
