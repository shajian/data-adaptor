package com.qianzhan.qichamao.dal;

import com.qianzhan.qichamao.util.DbConfigBus;
import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisClient implements AutoCloseable {
    private static JedisPool pool = null;

    private static Map<Integer, Jedis> map = new HashMap<>();

    public static int reverseIndexDb;
    @Getter
    private Jedis jedis = null;

    static {
        try {
            String host = DbConfigBus.getDbConfig_s("redis.host", "");
            int port = DbConfigBus.getDbConfig_i("redis.port", -1);
            String pwd = DbConfigBus.getDbConfig_s("redis.pass", "");
            reverseIndexDb = DbConfigBus.getDbConfig_i("redis.db.negative", 2);
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(1000);
            config.setMaxIdle(100);
            if (pwd != null && !pwd.equals(""))
                pool = new JedisPool(config, host, port, 10000, pwd);
            else
                pool = new JedisPool(config, host, port, 10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void registerClient(Integer dbIndex) {
        if (!map.containsKey(dbIndex)) {
            Jedis client = pool.getResource();
            client.select(dbIndex);
            map.put(dbIndex, client);
        }
    }

    public static Jedis get(Integer dbIndex) {
        if (!map.containsKey(dbIndex)) {
            Jedis client = pool.getResource();
            client.select(dbIndex);
            map.put(dbIndex, client);
            return client;
        }
        return map.get(dbIndex);
    }

    public static void unregisterClient(Integer dbIndex) {
        if (map.containsKey(dbIndex)) {
            Jedis client = map.get(dbIndex);
            client.close();
        }
    }


    public void close() {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String type(String key) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.type(key);
        }
    }

    /**
     * get a string value of `key`
     * @param key
     * @return
     */
    public static String get(String key) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.get(key);
        }
    }

    /**
     * set a string value to the `key`
     * @param key
     * @param value
     * @return string-type reply of this operation
     */
    public static String set(String key, String value) {
        try (RedisClient rc = new RedisClient()) {
            return rc.jedis.set(key, value);
        }
    }

    /**
     * del by an array of keys
     * @param keys
     * @return count of successfully deleted items
     */
    public static long del(String... keys) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.del(keys);
        }
    }

    /**
     *
     * @param key
     * @param value
     * @return total length of the string after the append operation
     */
    public static long append(String key, String value) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.append(key, value);
        }
    }

    public static boolean exists(String key) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.exists(key);
        }
    }

    public static long exists(String... keys) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.exists(keys);
        }
    }

    /**
     * multi-get: get values of multi-keys. Each key has only one single value.
     * @param keys e.g. "key1", "key2"
     * @return
     */
    public static List<String> mget(String... keys) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.mget(keys);
        }
    }


    /**
     *
     * @param keysValues e.g. "key1","value1","key2","value2"
     * @return simple string reply
     */
    public static String mset(String... keysValues) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.mset(keysValues);
        }
    }

    /**
     * this is an atomic operation and will not perform any operation at all
     * even if just a single key already exists
     * @param keysValues e.g. "key1","value1","key2","value2"
     * @return 1 if all the keys were set; 0 if on key was set(at least one key already existed)
     */
    public static long msetnx(String... keysValues) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.msetnx(keysValues);
        }
    }

    /**
     * set the string value of a key and return its old value
     * @param key
     * @param value
     * @return
     */
    public static String getSet(String key, String value) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.getSet(key, value);
        }
    }


    /**
     * return all members of a set identified by `key`
     * @param key
     * @return
     */
    public static Set<String> smembers(String key) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.smembers(key);
        }
    }

    /**
     * judge the `member` is the member of a set identified by `key`
     * @param key
     * @param member
     * @return
     */
    public static boolean sismember(String key, String member) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.sismember(key, member);
        }
    }

    /**
     * add members into a set identified by `key`
     * @param key
     * @param members
     * @return
     */
    public static long sadd(String key, String... members) {
        try(RedisClient rc = new RedisClient()) {
            return rc.jedis.sadd(key, members);
        }
    }
}
