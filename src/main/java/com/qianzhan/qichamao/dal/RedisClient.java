package com.qianzhan.qichamao.dal;

import com.qianzhan.qichamao.util.DbConfigBus;
import redis.clients.jedis.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisClient {
    private static JedisPool pool = null;

    public static int reverseIndexDb;

    static {
        try {
            String host = DbConfigBus.getDbConfig_s("redis.host", "");
            int port = DbConfigBus.getDbConfig_i("redis.port", -1);
            String pwd = DbConfigBus.getDbConfig_s("redis.pass", "");
            reverseIndexDb = DbConfigBus.getDbConfig_i("redis.db.negative", -1);
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

    public static String type(String key) {
//        try(RedisClient rc = new RedisClient()) {
//            return rc.jedis.type(key);
//        }
        throw new NotImplementedException();
    }

    /**
     * get a string value of `key`
     * @param key
     * @return
     */
    public static String get(String key) {
        return get(key, reverseIndexDb);
    }

    public static String get(String key, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        String value = jedis.get(key);
        jedis.close();
        return value;
    }

    /**
     * set a string value to the `key`
     * @param key
     * @param value
     * @return string-type reply of this operation
     */
    public static String set(String key, String value) {
        return set(key, value, reverseIndexDb);
    }

    public static String set(String key, String value, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        String status = jedis.set(key, value);
        jedis.close();
        return status;
    }

    /**
     * del by an array of keys
     * @param keys
     * @return count of successfully deleted items
     */
    public static long del(String... keys) {
        return del(reverseIndexDb, keys);
    }

    public static long del(int dbIndex, String... keys) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        long value = jedis.del(keys);
        jedis.close();
        return value;
    }

    /**
     *
     * @param key
     * @param value
     * @return total length of the string after the append operation
     */
    public static long append(String key, String value) {
        return append(key, value, reverseIndexDb);
    }

    public static long append(String key, String value, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        long length = jedis.append(key, value);
        jedis.close();
        return length;
    }

    public static boolean exists(String key) {
        return exists(key, reverseIndexDb);
    }

    public static boolean exists(String key, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        boolean b = jedis.exists(key);
        jedis.close();
        return b;
    }

    public static long exists(String... keys) {
        return exists(reverseIndexDb, keys);
    }

    public static long exists(int dbIndex, String... keys) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        long size = jedis.exists(keys);
        jedis.close();
        return size;
    }

    /**
     * multi-get: get values of multi-keys. Each key has only one single value.
     * @param keys e.g. "key1", "key2"
     * @return
     */
    public static List<String> mget(String... keys) {
        return mget(reverseIndexDb, keys);
    }

    public static List<String> mget(int dbIndex, String... keys) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        List<String> values = jedis.mget(keys);
        jedis.close();
        return values;
    }


    /**
     *
     * @param keysValues e.g. "key1","value1","key2","value2"
     * @return simple string reply
     */
    public static String mset(String... keysValues) {
        return mset(reverseIndexDb, keysValues);
    }

    public static String mset(int dbIndex, String... keysValues) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        String status = jedis.mset(keysValues);
        jedis.close();
        return status;
    }

    /**
     * this is an atomic operation and will not perform any operation at all
     * even if just a single key already exists
     * @param keysValues e.g. "key1","value1","key2","value2"
     * @return 1 if all the keys were set; 0 if on key was set(at least one key already existed)
     */
    public static long msetnx(String... keysValues) {
        return msetnx(reverseIndexDb, keysValues);
    }

    public static long msetnx(int dbIndex, String... keysValues) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        long size = jedis.msetnx(keysValues);
        jedis.close();
        return size;
    }

    /**
     * set the string value of a key and return its old value
     * @param key
     * @param value
     * @return
     */
    public static String getSet(String key, String value) {
        return getSet(key, value, reverseIndexDb);
    }

    public static String getSet(String key, String value, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        String old = jedis.getSet(key, value);
        jedis.close();
        return old;
    }


    /**
     * return all members of a set identified by `key`
     * @param key
     * @return
     */
    public static Set<String> smembers(String key) {
        return smembers(key, reverseIndexDb);
    }

    public static Set<String> smembers(String key, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        Set<String> values = jedis.smembers(key);
        jedis.close();
        return values;
    }

    /**
     * judge the `member` is the member of a set identified by `key`
     * @param key
     * @param member
     * @return
     */
    public static boolean sismember(String key, String member) {
        return sismember(key, member, reverseIndexDb);
    }

    public static boolean sismember(String key, String member, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        boolean b = jedis.sismember(key, member);
        jedis.close();
        return b;
    }

    /**
     * add members into a set identified by `key`
     * @param key
     * @param members
     * @return
     */
    public static long sadd(String key, String... members) {
        return sadd(reverseIndexDb, key, members);
    }

    public static long sadd(int dbIndex, String key, String... members) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        long size = jedis.sadd(key, members);
        jedis.close();
        return size;
    }

    public static Set<String> keys(String pattern) {
        return keys(pattern, reverseIndexDb);
    }
    public static Set<String> keys(String pattern, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        Set<String> set = jedis.keys(pattern);
        jedis.close();
        return set;
    }

    public static Set<String> scan(String pattern) {
        return scan(pattern, reverseIndexDb);
    }

    public static Set<String> scan(String pattern, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        ScanParams params = new ScanParams();
        params.count(5000).match("s:*");
        String cursor = "0";
        Set<String> keys = new HashSet<>();
        ScanResult<String> r;
        do {
            r = jedis.scan(cursor, params);
            cursor = r.getCursor();
            keys.addAll(r.getResult());
        } while (!cursor.equals("0"));
        jedis.close();
        return keys;
    }

    public static Set<String> sscan(String key) {
        return sscan(key, reverseIndexDb);
    }

    public static Set<String> sscan(String key, int dbIndex) {
        Jedis jedis = pool.getResource();
        jedis.select(dbIndex);
        String cursor = "0";
        Set<String> keys = new HashSet<>();
        ScanParams params = new ScanParams();
        params.count(5000);
        ScanResult<String> r;
        do {
            r = jedis.sscan(key, cursor, params);
            cursor = r.getCursor();
            keys.addAll(r.getResult());
        } while (!cursor.equals("0"));
        jedis.close();
        return keys;
    }
}
