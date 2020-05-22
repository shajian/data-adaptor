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
public abstract class ComBase implements Runnable {
    protected ComPack compack;
    protected String tasks_key;
    protected static Map<String, CountDownLatch> latches = new HashMap<>();

    public ComBase(ComPack cp) { compack = cp; }

    public ComBase(String tasks_key) {
        this.tasks_key = tasks_key;
        compack = SharedData.get(tasks_key);
    }

    public void countDown() {
        latches.get(this.tasks_key).countDown();
    }

    public static void resetLatch(String tasks_key, int count) {
        latches.put(tasks_key, new CountDownLatch(count));
    }

    public static CountDownLatch getLatch(String tasks_key) {
        return latches.get(tasks_key);
    }
}
