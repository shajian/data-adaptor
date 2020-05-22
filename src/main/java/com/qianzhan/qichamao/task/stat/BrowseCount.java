package com.qianzhan.qichamao.task.stat;

import com.qianzhan.qichamao.collection.AdaFitHistogram;
import com.qianzhan.qichamao.config.BaseConfigBus;
import com.qianzhan.qichamao.entity.OrgBrowseLog;
import com.qianzhan.qichamao.entity.RetrieveRange;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This task need to create a table in advance under the same directory of 'BrowseLog'. We
 * name the newed table as 'BrowseCount' which contains three field mainly:
 *      auto-incr id, oc_code, count
 */
public class BrowseCount {

    private static int checkpoint;
    private static final String checkpointName = "data-adaptor.mssql.browse_count";
    private static BaseConfigBus config;
    private static int batch;
    private static Map<String, Integer> buffer = new HashMap<>();
    private static int buffer_threshold;
    private static long interval;


    private static AdaFitHistogram histogram;

    /**
     * start the statistic task
     * statistic browsing count for each company
     * @throws Exception
     */
    public static void start() throws Exception {
        config = new BaseConfigBus("config/BrowseCount.txt");
        int state = config.getInt("state", 1);
        batch = config.getInt("batch", 10000);
        buffer_threshold = config.getInt("buffer_threshold", 10000);
        if (state == 1) {
            create();
        } else if (state == 2) {
            update();
        }
    }

    private static void create() {
        List<RetrieveRange> counts = MybatisClient.getBrowseCounts(0, batch +100);
        if (counts.size() <= batch && counts.size() > 0)
            MybatisClient.truncateBrowseCount();
        while (createInner()) {
            System.out.println(String.format("%s-checkpoint: %d @ %s",
                    Thread.currentThread().getStackTrace()[1].getClassName(),
                    checkpoint, new Date().toString()));
        }
        if (buffer.size() > 0) {
            flushBuffer();
        }
        // save checkpoint into database
        int old = MybatisClient.getCheckpoint(checkpointName);
        if (old < 0) {
            MybatisClient.insertCheckpoint(checkpointName, checkpoint);
        } else {
            MybatisClient.updateCheckpoint(checkpointName, checkpoint);
        }
    }

    private static boolean createInner() {
        List<OrgBrowseLog> logs = MybatisClient.getBrowseLogs(checkpoint, batch);
        for (OrgBrowseLog log : logs) {
            if (log.bl_id > checkpoint) checkpoint = log.bl_id;
            int old = buffer.getOrDefault(log.bl_oc_code, 0);
            buffer.put(log.bl_oc_code, old+1);
        }

        if (buffer.size() > buffer_threshold || logs.size() == 0) {
            System.out.println("try to flush buffer and it may take a long time, be patient...");
            flushBuffer();
            System.out.println("flush buffer finish.");
        }
        return logs.size()>0;
    }

    private static void flushBuffer() {
        int i = 0;
        for (String key : buffer.keySet()) {
            int old = MybatisClient.getBrowseCount(key);
            int cur = buffer.getOrDefault(key, 0);
            if (cur > 0) {
                if (old < 0) {
                    MybatisClient.insertBrowseCount(key, cur);
                } else {
                    MybatisClient.updateBrowseCount(key, cur+old);
                }
            }

            i++;
            if (i % 1000 == 0) {
                System.out.println("1000 items have been save into database @ "+new Date().toString());
            }
        }
        buffer.clear();
    }

    private static void update() throws Exception {
        interval = config.getInt("interval", 1) * 1000L * 3600 * 24;
        checkpoint = MybatisClient.getCheckpoint(checkpointName);
        if (checkpoint < 0) {
            MybatisClient.insertCheckpoint0(checkpointName);
            checkpoint = 0;
        }
        while (true) {
            while(updateInner()) {
                System.out.println(String.format("checkpoint %d @ %s", checkpoint, new Date().toString()));
            }
            Date now = new Date();
            Date end = new Date(now.getTime()+interval);
            System.out.println(String.format("enter sleeping mode @ %s and it will wake up @ %s", now, end));
            Thread.sleep(interval);
        }
    }

    private static boolean updateInner() {
        boolean r = createInner();
        MybatisClient.updateCheckpoint(checkpointName, checkpoint);
        return r;
    }

    /**
     * On the other hand, we can load from database and fill into a AdaFitHistogram instance,
     * and later get scores based on it.
     * Because browse logs are changing all the time, so I do not cache it into a file.
     * It reloads from database every time to get the newest data when parent-task relaunched.
     */
    public static void initialize() throws Exception {
        histogram = new AdaFitHistogram(5);
        int start = 0;
        int end = initInner(start);
        while (end > 0) {
            start = end;
            System.out.println(String.format("checkpoint: %d @ %s", start, new Date().toString()));
            end = initInner(start);
        }
        histogram.gen();
    }

    private static int initInner(int start) {
        List<RetrieveRange> ranges = MybatisClient.getBrowseCounts(start, 8000);
        if (ranges.size() > 0) {
            int[] cs = new int[ranges.size()];
            for (int i = 0; i < ranges.size(); ++i) {
                cs[i] = ranges.get(i).getCount();
            }
            histogram.adapt(cs);
            return ranges.get(ranges.size()-1).getStart();
        }
        return 0;
    }

    public static int getScore(int count) {
        return histogram.getScore(count);
    }
}
