package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.BaseConfigBus;
import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import lombok.Setter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class MainTaskBase {
    protected int checkpoint = 0;
    protected String checkpointName;
    protected BaseConfigBus config;
    protected TaskType task;
    protected List<ComHook> preHooks;
    protected List<ComHook> postHooks;
    // whether app was closed manually
    @Setter
    private boolean shutdown;
    // notice outer that current task had been exited safely.
    @Setter
    private boolean notice;
    protected int batch;
    private Pattern[] filter_outs;
    /**
     * 1. launch task from original point
     * 2. launch task for update
     */
    protected int state;
    protected int iter_print_interval;
    protected ThreadPoolExecutor pool;
    protected int max_threads;
    protected int thread_queue_size_ratio;


    public MainTaskBase(String file) throws Exception {
        File f = new File(file);

        String[] parts = f.getName().split("_");
        task = TaskType.valueOf(parts[1].toLowerCase());
        config = new BaseConfigBus(file);
        int env = config.getInt("env", -1);
        if (env > 0) GlobalConfig.setEnv((byte)env);
        batch = config.getInt("batch", 1000);
        state = config.getInt("state", 1);
        iter_print_interval = config.getInt("iter_print_interval", 0);
        String[] filter_outs_str = config.getString("filter_out").split("\\s");
        filter_outs = new Pattern[filter_outs_str.length];
        for (int i = 0; i < filter_outs_str.length; ++i) {
            filter_outs[i] = Pattern.compile(String.format("^%s$", filter_outs_str[i]));
        }
        SharedData.registerConfig(task, config);

        if (config.getBool("multi_thread", false)) {
            thread_queue_size_ratio = config.getInt("thread_queue_size_ratio", 5);
            max_threads = config.getInt("max_threads", 0);
            if (max_threads <= 0) {
                max_threads = Runtime.getRuntime().availableProcessors() * 15;
            }
            BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>(thread_queue_size_ratio*batch);
            pool = new ThreadPoolExecutor(max_threads, max_threads, 0, TimeUnit.SECONDS,
                    queue, new ThreadPoolExecutor.AbortPolicy());
        }
    }

    public void start() {
        try {
            exec_state();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void state1_pre() throws Exception { }
    protected void state2_pre() throws Exception { }
    protected void state3_pre() throws Exception { }
    protected void state4_pre() throws Exception { }
    protected void state5_pre() throws Exception { }
    protected void state6_pre() throws Exception { }

    protected void state1_post() throws Exception { }
    protected void state2_post() throws Exception { }
    protected void state3_post() throws Exception { }
    protected void state4_post() throws Exception { }
    protected void state5_post() throws Exception { }
    protected void state6_post() throws Exception { }
    protected boolean state1_inner() throws Exception {
        throw new NotImplementedException();
    }
    protected boolean state2_inner() throws Exception {
        throw new NotImplementedException();
    }
    protected boolean state3_inner() throws Exception {
        throw new NotImplementedException();
    }
    protected boolean state4_inner() throws Exception {
        throw new NotImplementedException();
    }
    protected boolean state5_inner() throws Exception {
        throw new NotImplementedException();
    }
    protected boolean state6_inner() throws Exception {
        throw new NotImplementedException();
    }

    private void exec_state() throws Exception {
        if (state == 1) {
            state1_pre();
        } else if (state == 2) {
            state2_pre();
        } else if (state == 3) {
            state3_pre();
        } else if (state == 4) {
            state4_pre();
        } else if (state == 5) {
            state5_pre();
        } else if (state == 6) {
            state6_pre();
        }

        state_main();

        if (state == 1) {
            state1_post();
        } else if (state == 2) {
            state2_post();
        } else if (state == 3) {
            state3_post();
        } else if (state == 4) {
            state4_post();
        } else if (state == 5) {
            state5_post();
        } else if (state == 6) {
            state6_post();
        }
    }

    private void state_main() throws Exception {
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
        System.out.println("start to state...");
        int num = 0;
        while (state_iter()) {
            num++;
            if (iter_print_interval == 0 || num % iter_print_interval == 0) {
                String suffix = "\033[0m";
                String prefix = num % 2 == 0 ? "\033[35;4m" : "\033[34;4m";

                System.out.println(String.format("%s-- state: %d, %s: %d @ %s --%s",
                        prefix, state, checkpointName, checkpoint, new Date(), suffix));
            }
            if (shutdown) {
                notice = true;
                break;
            }
        }
    }

    public void exitSafely() {
        shutdown = true;
        System.out.println("app will be closed, please wait...");
        int i = 0;
        try {
            while (i < 10) {
                if (notice) break;
                Thread.sleep(500);
                i++;
            }
            if (!notice) {
                // if has not received notice, wait for another 2s
                System.out.println("app will exit after 5s");
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // exit
    }

    private boolean state_iter() {
        if (preHooks != null) {
            for (ComHook hook : preHooks) {
                hook.run();
            }
        }
        boolean res = false;
        try {
            if (state == 1) {
                res = state1_inner();
            } else if (state == 2) {
                res = state2_inner();
            } else if (state == 3) {
                res = state3_inner();
            } else if (state == 4) {
                res = state4_inner();
            } else if (state == 5) {
                res = state5_inner();
            } else if (state == 6) {
                res = state6_inner();
            }
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




    public boolean filter_out(String name) {
        for (Pattern pattern : filter_outs) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.matches()) return true;
        }
        return false;
    }

    public boolean validateCode(String code) {
        if (MiscellanyUtil.isBlank(code) || code.length() != 9) return false;
        char codeTail = code.charAt(8);
        if (codeTail == 'T' || codeTail == 'K') {
            return false;
        }
        for (char c : code.toCharArray()) {
            if (!(c >= '0' && c <= '9') && !(c >= 'A' && c <= 'Z')) return false;
        }
        return true;
    }
}
