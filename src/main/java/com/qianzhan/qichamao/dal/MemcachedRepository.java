package com.qianzhan.qichamao.dal;

import com.qianzhan.qichamao.util.DbConfigBus;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MemcachedRepository {
    private MemcachedClient client;
    private MemcachedRepository() throws IOException {
        String host = DbConfigBus.getDbConfig_s("memcached.host", "");
        int port = DbConfigBus.getDbConfig_i("memcached.port", 11211);
        client = new MemcachedClient(new InetSocketAddress(host, port));
    }
    private static MemcachedRepository _singleton;
    public static MemcachedRepository singleton() throws IOException {
        if (_singleton == null) _singleton = new MemcachedRepository();
        return _singleton;
    }

    public OperationFuture<Boolean> aset(String key, Object value) {
        return client.set(key, 0, value);
    }
    public OperationFuture<Boolean> aset(String key, int exptime, Object value) {
        return client.set(key, exptime, value);
    }
    public boolean set(String key, Object value) throws Exception {
        Future<Boolean> r = client.set(key, 0, value);
        return r.get();
    }

    public boolean set(String key, int exptime, Object value) throws Exception {
        Future<Boolean> r = client.set(key, exptime, value);
        return r.get();
    }


    public Object get(String key) {
        return client.get(key);
    }

    public GetFuture<Object> aget(String key) {
        return client.asyncGet(key);
    }

    public OperationFuture<Boolean> adelete(String key) {
        return client.delete(key);
    }
    public boolean delete(String key) throws Exception {
        return client.delete(key).get();
    }

    public void close() {
        client.shutdown();
    }
}
