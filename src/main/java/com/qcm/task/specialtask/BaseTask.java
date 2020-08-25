package com.qcm.task.specialtask;

import com.qcm.task.maintask.ComPack;
import com.qcm.task.maintask.SharedData;
import com.qcm.task.maintask.TaskType;
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
public abstract class BaseTask implements Runnable {
    protected ComPack compack;
    protected TaskType task;
    protected static Map<TaskType, CountDownLatch> latches = new HashMap<>();

    public BaseTask(ComPack cp) { compack = cp; task = cp.task; }

    public BaseTask(TaskType task) {
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
