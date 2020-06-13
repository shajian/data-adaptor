package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.BaseConfigBus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SharedData {
    // this cache is instantaneous
    // key: tasks_key, value: ComPack
    private static Map<String, ComPack> packs = new HashMap<>();
    // each item of this cache has an iterated lifetime
    private static Map<String, List<ComPack>> packss = new HashMap<>();

    // this cache has a app-process lifetime
    private static Map<String, BaseConfigBus> configs = new HashMap<>();

    public static void registerConfig(String key, BaseConfigBus config) {
        configs.put(key, config);
    }


    // ============ synchronization depends on user ==============
    public static void openBatch(String key) {
        if (!packss.containsKey(key)) {
            packss.put(key, new ArrayList<>());
        }
    }

    public static void closeBatch(String key) {
        packss.get(key).clear();
    }

    public static void open(String key) throws Exception {
        ComPack cp = new ComPack(key);
        packs.put(key, cp);
        packss.get(key).add(cp);
    }

    public static void close(String key) {
        packs.put(key, null);
    }

    public static ComPack get(String key) {
        return packs.get(key);
    }

    public static List<ComPack> getBatch(String key) {
        return packss.get(key);
    }
    public static BaseConfigBus getConfig(String key) {
        return configs.get(key);
    }
}
