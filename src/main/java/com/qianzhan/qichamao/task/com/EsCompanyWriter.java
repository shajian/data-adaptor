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

    private static Pattern[] filter_outs;

    private static CompletionService<Boolean> threadpool;
    private static int max_threads;
    private static int thread_queue_size_ratio;
    private static int thread_active_threshold_ratio;
    private static final Logger logger = LoggerFactory.getLogger(EsCompanyWriter.class);



    public EsCompanyWriter() throws Exception {
        super("config/EsCompany.txt");
        // initialize ES read/write components
        repository = new EsCompanyRepository();
        checkpointName = "data-adaptor.es."+repository.getIndexMeta().index();

        String[] filter_outs_str = config.getString("filter_out").split("\\s");
        filter_outs = new Pattern[filter_outs_str.length];
        for (int i = 0; i < filter_outs_str.length; ++i) {
            filter_outs[i] = Pattern.compile(String.format("^%s$", filter_outs_str[i]));
        }
        thread_queue_size_ratio = config.getInt("thread_queue_size_ratio", 5);
        thread_active_threshold_ratio = config.getInt("thread_active_threshold_ratio", 2);
        max_threads = config.getInt("max_threads", 0);
        if (max_threads <= 0) {
            max_threads = Runtime.getRuntime().availableProcessors() * 4;
        }
        BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>(max_threads*thread_queue_size_ratio);
        Executor executor = new ThreadPoolExecutor(max_threads, max_threads, 0, TimeUnit.SECONDS,
                queue, new ThreadPoolExecutor.AbortPolicy());
        threadpool = new ExecutorCompletionService<Boolean>(executor);
    }

    private void preCreate() throws Exception {
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

    private boolean createInner() throws Exception{
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;    // task finishes !!!

        int running = 0;
        int success = 0;
        int failure = 0;
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

            threadpool.submit(new ComDtl(tasks_key));
            threadpool.submit(new ComMember(tasks_key));
            threadpool.submit(new ComShareHolder(tasks_key));

            threadpool.submit(new ComContact(tasks_key));
            threadpool.submit(new ComOldName(tasks_key));
            threadpool.submit(new ComIndustry(tasks_key));

            threadpool.submit(new ComGeo(tasks_key));
            threadpool.submit(new ComBrand(tasks_key));
            threadpool.submit(new ComTag(tasks_key));

            SharedData.close(tasks_key);

            running += 9;
            while (running > max_threads*thread_active_threshold_ratio) {
                Boolean b = threadpool.take().get();
                if (b) success++;
                else failure++;
                running--;
            }
        }
        while(running > 0) {
            Boolean b = threadpool.take().get();
            if (b) success++;
            else failure++;
            running--;
        }

        // set weight and score
        for (ComPack cp : SharedData.getBatch(tasks_key)) {
            threadpool.submit(new ComScore(cp));
            running++;
            while (running > max_threads*thread_active_threshold_ratio) {
                Boolean b = threadpool.take().get();
                if (b) success++;
                else failure++;
                running--;
            }
        }

        // wait until all scores are calculated
        while (running>0) {
            Boolean b = threadpool.take().get();
            if (b) success++;
            else failure++;
            running--;
        }



        System.out.println(String.format("total sub-thread tasks: %d, success: %d, failure: %d",
                6 * companies.size(), success, failure));
        System.out.println("writing into ES...");

        List<EsCompany> e_coms = new ArrayList<>();
        for (ComPack cp : SharedData.getBatch(tasks_key)) {
            e_coms.add(cp.e_com);
        }
        repository.index(e_coms);

        MybatisClient.updateCheckpoint(checkpointName, checkpoint);
        return true;
    }

    public static boolean filter_out(String name) {
        for (Pattern pattern : filter_outs) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) return true;
        }
        return false;
    }
}
