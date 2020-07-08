package com.qcm.dal;

import com.qcm.util.DbConfigBus;
import com.qcm.util.MiscellanyUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;

public class MemcachedRepository {
    private MemcachedClient client;
    private MemcachedRepository() throws IOException {
        String host = DbConfigBus.getDbConfig_s("memcached.host", "");
        int port = DbConfigBus.getDbConfig_i("memcached.port", 11211);
        if (MiscellanyUtil.isBlank(host)) return;
        try {
            client = new MemcachedClient(new InetSocketAddress(host, port));
        } catch (Exception e) {
            //
        }
    }
    private static MemcachedRepository _singleton;
    public static MemcachedRepository singleton() throws IOException {
        if (_singleton == null) _singleton = new MemcachedRepository();
        return _singleton;
    }

    public OperationFuture<Boolean> aset(String key, Object value) {
        if (client != null) {
            return client.set(key, 0, value);
        }
        return null;
    }
    public OperationFuture<Boolean> aset(String key, int exptime, Object value) {
        if (client != null) {
            return client.set(key, exptime, value);
        }
        return null;
    }
    public boolean set(String key, Object value) throws Exception {
        if (client == null) return false;
        Future<Boolean> r = client.set(key, 0, value);
        return r.get();
    }

    public boolean set(String key, int exptime, Object value) throws Exception {
        if (client == null) return false;
        Future<Boolean> r = client.set(key, exptime, value);
        return r.get();
    }


    public Object get(String key) {
        if (client == null) return null;
        return client.get(key);
    }

    public GetFuture<Object> aget(String key) {
        if (client == null) return null;
        return client.asyncGet(key);
    }

    public OperationFuture<Boolean> adelete(String key) {
        if (client == null) return null;
        return client.delete(key);
    }
    public boolean delete(String key) throws Exception {
        if (client == null) return false;
        return client.delete(key).get();
    }

    public void close() {
        client.shutdown();
    }
}
