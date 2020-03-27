package com.qianzhan.qichamao.util;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DbConfigBus {
    private static Properties dbConfig;
    private static Lock lock = new ReentrantLock();
    public static String getDbConfig_s(String key, String def) {
        InputStream is = null;
        if (dbConfig == null) {
            lock.lock();
            if (dbConfig == null) {
                try {
//                    is = new FileInputStream("db.properties");
                    is = DbConfigBus.class.getResourceAsStream("/db.properties");
                    dbConfig = new Properties();
                    dbConfig.load(is);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (is != null)
                        try {
                            is.close();
                        } catch (IOException e) {
                            // todo log
                        }
                }
            }
            lock.unlock();
        }

        return dbConfig.getProperty(key, def);
    }

    public static int getDbConfig_i(String key, int def) {
        String val = getDbConfig_s(key, null);
        if (val == null) return def;

        return Integer.getInteger(key, def);
    }

    public static String[] getDbConfig_ss(String key, String[] def) {
        String val = getDbConfig_s(key, null);
        if (val == null) {
            return def;
        }
        return val.split(",");
    }
}
