package com.qcm.util;

import com.qcm.config.GlobalConfig;

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
                    String configFile = "/db.properties.private";
                    if (GlobalConfig.getEnv() == 1) {   // local env.
                        is = DbConfigBus.class.getResourceAsStream(configFile);
                    } else {
                        // do not read config file from jar, since it is very troublesome to change some configurations.
                        // read config file in the same directory of jar.
                        configFile = MiscellanyUtil.jarDir() + configFile;
                        is = new FileInputStream(new File(configFile));
                    }
                    System.out.println("db config file: "+configFile);
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
                            e.printStackTrace();
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

        return Integer.parseInt(val);
    }

    public static String[] getDbConfig_ss(String key, String[] def) {
        String val = getDbConfig_s(key, null);
        if (val == null) {
            return def;
        }
        return val.split(",");
    }
}
