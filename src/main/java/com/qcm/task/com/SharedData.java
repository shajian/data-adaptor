package com.qcm.task.com;

import com.qcm.config.BaseConfigBus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SharedData {
    // this cache is instantaneous
    // key: task, value: ComPack
    private static Map<TaskType, ComPack> packs = new HashMap<>();
    // each item of this cache has an iterated lifetime
    private static Map<TaskType, List<ComPack>> packss = new HashMap<>();

    // this cache has a app-process lifetime
    private static Map<TaskType, BaseConfigBus> configs = new HashMap<>();

    public static void registerConfig(TaskType key, BaseConfigBus config) {
        configs.put(key, config);
    }


    // ============ synchronization depends on user ==============
    public static void openBatch(TaskType key) {
        if (!packss.containsKey(key)) {
            packss.put(key, new ArrayList<>());
        }
    }

    public static void closeBatch(TaskType key) {
        packss.get(key).clear();
    }

    public static void open(TaskType key) throws Exception {
        ComPack cp = new ComPack(key);
        packs.put(key, cp);
        packss.get(key).add(cp);
    }

    public static void close(TaskType key) {
        packs.put(key, null);
    }

    public static ComPack get(TaskType key) {
        return packs.get(key);
    }

    public static List<ComPack> getBatch(TaskType key) {
        return packss.get(key);
    }
    public static BaseConfigBus getConfig(TaskType key) {
        return configs.get(key);
    }
}
