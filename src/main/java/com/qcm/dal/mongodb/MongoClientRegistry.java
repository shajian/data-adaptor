package com.qcm.dal.mongodb;


import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Mongo Client at the grain of Collection
 */
public class MongoClientRegistry {
    private static Map<String, MongodbClient> map = new HashMap<>();
    private static Lock lock = new ReentrantLock();

    public static MongodbClient client(CollName name) {
        return client(name.name());
    }

    public static MongodbClient client(String name) {
        if (!map.containsKey(name)) {
            lock.lock();
            if (!map.containsKey(name)) {
                MongodbClient c = new MongodbClient(name);
                map.put(name, c);
            }
            lock.unlock();
        }
        return map.get(name);
    }


    public static void register(String name, MongodbClient client) {
        if (map.containsKey(name)) {
            throw new ValueException(String.format("client name %s has been registered.", name));
        }
        map.put(name, client);
    }

    public enum CollName {
        dtl,
        person,
        sharename,
    }
}
