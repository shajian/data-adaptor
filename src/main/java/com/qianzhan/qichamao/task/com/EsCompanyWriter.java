package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.dal.mongodb.MongodbClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.MongoCompany;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.EsConfigBus;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
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
public class EsCompanyWriter {
    private static EsCompanyRepository repository;
    private static int checkpoint;
    private static String checkpointName;
    private static int batchsize = 1000;

    private static Pattern[] filter_outs;


    private static CompletionService<Boolean> threadpool;
    private static int max_threads;
    private static int thread_queue_size_ratio;
    private static int thread_active_threshold_ratio;
    private static final Logger logger = LoggerFactory.getLogger(EsCompanyWriter.class);

    public static void start() {
        try {
            // initialize ES read/write components
            repository = new EsCompanyRepository();
            checkpointName = "data-adaptor."+repository.getIndexMeta().index();

            String[] filter_outs_str = EsConfigBus.getTaskConfigString("filter_out").split("\\s");
            filter_outs = new Pattern[filter_outs_str.length];
            for (int i = 0; i < filter_outs_str.length; ++i) {
                filter_outs[i] = Pattern.compile(String.format("^%s$", filter_outs_str[i]));
            }
            batchsize = EsConfigBus.getTaskConfigInt("batch");
            int state = EsConfigBus.getTaskConfigInt("state");
            thread_queue_size_ratio = EsConfigBus.getTaskConfigInt("thread_queue_size_ratio");
            thread_active_threshold_ratio = EsConfigBus.getTaskConfigInt("thread_active_threshold_ratio");
            max_threads = EsConfigBus.getTaskConfigInt("max_thread_nums");
            if (max_threads <= 0) {
                max_threads = Runtime.getRuntime().availableProcessors() * 4;
            }
            BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>(max_threads*thread_queue_size_ratio);
            Executor executor = new ThreadPoolExecutor(max_threads, max_threads, 0, TimeUnit.SECONDS,
                    queue, new ThreadPoolExecutor.AbortPolicy());
            threadpool = new ExecutorCompletionService<Boolean>(executor);



            if (state == 1) {
                // create
                create();
            } else if(state == 2) {
                // update
            } // other customized action
        } catch (Exception e) {
            // todo log
            logger.error(e.getMessage());
        }
    }

    private static void create() throws Exception {
        checkpoint = MybatisClient.getCheckpoint(checkpointName);
        if (checkpoint < 0) {
            MybatisClient.insertCheckpoint0(checkpointName);
            checkpoint = 0;
        }
        System.out.print("whether create ES mapping (Yy|Nn)?");
        char c = (char) System.in.read();
        if (c == 'Y' || c == 'y') {
            System.out.println("checkpoint will be reset to 0.");
            if (repository.exists()) {
                System.out.print("index is already existed, delete and recreate it (Yy|Nn)?");
                c = (char) System.in.read();
                if (c == 'Y' || c == 'y') {
                    repository.delete();
                    Thread.sleep(1000);
                    repository.map();
                    // todo log
                }
            } else {
                repository.map();
                // todo log
            }
            checkpoint = 0;     // need not to reset back to database, because each loop will reset checkpoint too.
        } else if (checkpoint > 0) {
            System.out.print("reset checkpoint (Yy|Nn)?");
            c = (char) System.in.read();
            if (c == 'Y' || c == 'y') {
                checkpoint = 0; // need not to reset back to database, because each loop will reset checkpoint too.
                // todo log
            } else {
                // retrieve checkpoint from database
                checkpoint = MybatisClient.getCheckpoint(checkpointName);
                // todo log
            }
        }

        System.out.println("begin to write data into ES...");
        Thread.sleep(1000);

        while (createInner()) {
            System.out.println(String.format("checkpoint: %d @ %s", checkpoint, new Date().toString()));
        }
    }

    private static boolean createInner() throws Exception{
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batchsize);
        if (companies.size() == 0) return false;    // task finishes !!!

        List<EsCompany> coms = new ArrayList<>(companies.size());
        List<MongoCompany> es_complements = new ArrayList<>(companies.size());
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

            EsCompany c = new EsCompany();
            c.loadFrom(company);
            coms.add(c);
            MongoCompany es_c = new MongoCompany();
            es_complements.add(es_c);

            threadpool.submit(new EsComDtl() {{init(c, es_c);}});
            threadpool.submit(new EsComMember() {{init(c, es_c);}});
            threadpool.submit(new EsComShareHolder() {{init(c, es_c);}});

            threadpool.submit(new EsComContact(){{init(c, es_c);}});
            threadpool.submit(new EsComOldName(){{init(c, es_c);}});
            threadpool.submit(new EsComIndustry(){{init(c, es_c);}});

            threadpool.submit(new EsComGeo(){{init(c, es_c);}});
            threadpool.submit(new EsComBrand(){{init(c, es_c);}});
            threadpool.submit(new EsComTag(){{init(c, es_c);}});

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
        for (EsCompany c : coms) {
            threadpool.submit(new EsComScore(){{init(c, null);}});
            running++;
            while (running > max_threads*thread_active_threshold_ratio) {
                Boolean b = threadpool.take().get();
                if (b) success++;
                else failure++;
                running--;
            }
        }
        while (running>0) {
            Boolean b = threadpool.take().get();
            if (b) success++;
            else failure++;
            running--;
        }



        System.out.println(String.format("total sub-thread tasks: %d, success: %d, failure: %d",
                6 * companies.size(), success, failure));
        System.out.println("writing into ES...");

        System.out.println("return directly. this is for testing");
        return false;
//        repository.index(coms);
//
//        System.out.println("writing into mongodb...");
//        List<Document> docs = new ArrayList<>(es_complements.size());
//        for (MongoCompany es_c : es_complements) {
//            docs.add(BeanUtil.obj2Doc(es_c));
//        }
//        MongodbClient.insert(docs);
//
//        MybatisClient.updateCheckpoint(checkpointName, checkpoint);
//        return true;
    }

    public static boolean filter_out(String name) {
        for (Pattern pattern : filter_outs) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) return true;
        }
        return false;
    }
}
