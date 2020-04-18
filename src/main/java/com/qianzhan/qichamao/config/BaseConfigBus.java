package com.qianzhan.qichamao.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Every task should has its own configurations which are listed in some '.txt' files.
 * These configuration files are easy to modify whenever you want.
 * This kind of configurations are red only once at the launching of tasks, so when you change some
 * configurations, they only take effect after relaunching tasks.
 *
 * This is the base class, and many primary operations are provided here. If you want more
 * customize methods, please extends this class.
 */
public class BaseConfigBus {
    // lazy loading
    private Map<String, String> map; // = new HashMap<>();

    public BaseConfigBus(String file) throws Exception {
        init(file);
    }
    private void init(String file) throws Exception {
        map = new HashMap<>();
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(fis, "utf-8");
        Properties p = new Properties();
        p.load(reader);
        for (String key : p.stringPropertyNames()) {
            map.put(key, p.getProperty(key));
        }
        fis.close();
    }

    public String getString(String key) throws Exception {
        return map.get(key);
    }

    public int getInt(String key, int def) throws Exception {
        String r = getString(key);
        if (r == null) return def;
        return Integer.parseInt(r);
    }

    public boolean getBool(String key) throws Exception {
        return Boolean.parseBoolean(getString(key));
    }

    public float getFloat(String key, float def) throws Exception {
        String r = getString(key);
        if (r == null) return def;
        return Float.parseFloat(r);
    }

    public int[] getInts(String key) throws Exception {
        String[] rs = getString(key).split(",");
        int[] ints = new int[rs.length];
        for (int i = 0; i < rs.length; ++i) {
            ints[i] = Integer.parseInt(rs[i]);
        }
        return  ints;
    }
}
