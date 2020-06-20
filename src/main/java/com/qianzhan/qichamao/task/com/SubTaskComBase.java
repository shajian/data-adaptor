package com.qianzhan.qichamao.task.com;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Base class, acts as a data pack.
 * Each subclass inherited this base class fills a special part of fields of
 * a company.
 */
@Getter@Setter
public abstract class SubTaskComBase implements Runnable {
    protected ComPack compack;
    protected TaskType task;
    protected static Map<TaskType, CountDownLatch> latches = new HashMap<>();

    public SubTaskComBase(ComPack cp) { compack = cp; task = cp.task; }

    public SubTaskComBase(TaskType task) {
        this.task = task;
        compack = SharedData.get(task);
    }

    public void countDown() {
        latches.get(this.task).countDown();
    }

    public static void resetLatch(TaskType task, int count) {
        latches.put(task, new CountDownLatch(count));
    }

    public static CountDownLatch getLatch(TaskType task) {
        return latches.get(task);
    }
}
