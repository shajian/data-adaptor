package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.BaseConfigBus;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public abstract class BaseWriter {
    protected int checkpoint = 0;
    protected String checkpointName;
    protected BaseConfigBus config;
    protected String tasks_key;
    protected List<ComHook> preHooks;
    protected List<ComHook> postHooks;
    protected int tasktype = 0;
    /**
     * 1. launch task from original point
     * 2. launch task for update
     */
    protected int state;

    public BaseWriter(String file) throws Exception {
        config = new BaseConfigBus(file);
        state = config.getInt("state", 1);
        tasks_key = config.getString("tasks_key");
        int[] tasks = config.getInts("tasks");
        for (int task : tasks) {
            tasktype |= task;
        }
        ComPack.registerTasktype(config.getString("tasks_key"), tasktype);
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

        }
    }

    private void preCreate() {
        // do some preparing work here before creation
    }

    private void create() throws IOException {
        preCreate();

        checkpoint = MybatisClient.getCheckpoint(checkpointName);
        if (checkpoint < 0) {
            MybatisClient.insertCheckpoint0(checkpointName);
            checkpoint = 0;
            System.out.println(String.format("create a new checkpoint: %s", checkpointName));
        } else {
            System.out.print("reset checkpoint (Yy|Nn)?");
            char c = (char) System.in.read();
            if (c == 'Y' || c == 'y') {
                checkpoint = 0; // need not to reset back to database, because each loop will reset checkpoint too.
                System.out.println("checkpoint reset to 0");
            }
        }
        System.out.println("start to create...");
        while (createIter()) {
            System.out.println(String.format("create: checkpoint: %d @ %s", checkpoint, new Date()));
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

        boolean res = createInner();
        if (postHooks != null) {
            for (ComHook hook : postHooks) {
                hook.run();
            }
        }
        return res;
    }
    private boolean createInner() {
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


}
