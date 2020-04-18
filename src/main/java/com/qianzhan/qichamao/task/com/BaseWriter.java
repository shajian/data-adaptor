package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.BaseConfigBus;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseWriter {
    protected int checkpoint = 0;
    protected String checkpointName;
    protected BaseConfigBus config;
    protected String tasks_key;
    protected List<ComHook> preHooks;
    protected List<ComHook> postHooks;
    protected int[] tasks;
    protected int batch;
    private Pattern[] filter_outs;
    /**
     * 1. launch task from original point
     * 2. launch task for update
     */
    protected int state;
    protected int iter_print_interval;

    public BaseWriter(String file) throws Exception {
        config = new BaseConfigBus(file);
        batch = config.getInt("batch", 1000);
        state = config.getInt("state", 1);
        tasks_key = config.getString("tasks_key");
        tasks = config.getInts("tasks");
        iter_print_interval = config.getInt("iter_print_interval", 10);
        String[] filter_outs_str = config.getString("filter_out").split("\\s");
        filter_outs = new Pattern[filter_outs_str.length];
        for (int i = 0; i < filter_outs_str.length; ++i) {
            filter_outs[i] = Pattern.compile(String.format("^%s$", filter_outs_str[i]));
        }
        SharedData.registerConfig(tasks_key, config);
        ComPack.registerTasktype(config.getString("tasks_key"), tasks);
    }

    public void start() {
        try {
            if (state == 1) {
                create();
            } else if (state == 2) {
                update();
            } else if (state == 3) {

            }
            System.out.println("task finished");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void preCreate() throws Exception {
        // do some preparing work here before creation
    }

    private void create() throws Exception {
        preCreate();

        checkpoint = MybatisClient.getCheckpoint(checkpointName);
        if (checkpoint < 0) {
            MybatisClient.insertCheckpoint0(checkpointName);
            checkpoint = 0;
            System.out.println(String.format("create a new checkpoint: %s", checkpointName));
        } else {
            System.out.print(String.format("%s: current checkpoint is %d, reset checkpoint (Yy|Nn)?",
                    checkpointName, checkpoint));
            char c = (char) System.in.read();
            if (c == 'Y' || c == 'y') {
                checkpoint = 0; // need not to reset back to database, because each loop will reset checkpoint too.
                System.out.println("checkpoint reset to 0");
            }
        }
        System.out.println("start to create...");
        int num = 0;
        while (createIter()) {
            num++;
            if (iter_print_interval == 0 || num % iter_print_interval == 0) {
                System.out.println(String.format("create: (%s, %d) @ %s", checkpointName, checkpoint, new Date()));
            }
        }

        postCreate();
    }

    private void postCreate() {
        // do some posting work here after creation.
    }

    private boolean createIter() {
        if (preHooks != null) {
            for (ComHook hook : preHooks) {
                hook.run();
            }
        }
        boolean res = false;
        try {
            res = createInner();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (postHooks != null) {
            for (ComHook hook : postHooks) {
                hook.run();
            }
        }
        MybatisClient.updateCheckpoint(checkpointName, checkpoint);
        return res;
    }
    protected boolean createInner() throws Exception {
        throw new NotImplementedException();
    }
    private void preUpdate() {
        // do some preparing work here before update.
    }

    private void update() {
        preUpdate();

        while (updateIter()) {
            System.out.println(String.format("update: checkpoint: %d @ %s", checkpoint, new Date()));
        }

        postUpdate();
    }

    private boolean updateIter() {
        throw new NotImplementedException();
    }

    private void postUpdate() {
        // do some posting work here after update.
    }


    public boolean filter_out(String name) {
        for (Pattern pattern : filter_outs) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.matches()) return true;
        }
        return false;
    }
}
