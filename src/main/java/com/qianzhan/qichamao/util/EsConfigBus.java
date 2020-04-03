package com.qianzhan.qichamao.util;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EsConfigBus {
    private static Lock lock = new ReentrantLock();
    private static Map<String, Object> map;
    private static Map<String, String> taskConfig;

    private static void parse() {
        if (map == null) {
            lock.lock();
            if (map == null) {
                InputStream is = null;
                try {
                    Yaml yaml = new Yaml();
                    is = EsConfigBus.class.getClassLoader().getResourceAsStream("EsConfig.yaml");
                    map = yaml.loadAs(is, Map.class);
                } catch (Exception e) {

                } finally {
                    if (is != null) {
                        try {
                            is.close();
                        } catch (IOException e) {

                        }
                    }
                }
            }
            lock.unlock();
        }
    }

    public static <T> T get(String key) {
        parse();
        String[] segs = key.split("\\.");
        Map<String, Object> m = map;
        for (int i = 0; i < segs.length -1; ++i) {
            if (m != null) {
                m = (Map<String, Object>) m.get(segs[i]);
            } else {
                break;
            }
        }
        if (m != null) {
            return (T) m.get(segs[segs.length-1]);
        }
        return null;
    }

//    public static String getTaskConfigString(String key) throws IOException {
//        //
//        if (taskConfig == null) {
//            taskConfig = new HashMap<>();
//            Properties p = new Properties();
//            FileInputStream fis = new FileInputStream("config/EsCompany.txt");
//            InputStreamReader reader = new InputStreamReader(fis, "utf-8");
//            p.load(reader);
//
//            reader.close();
//            fis.close();
//            for (String name : p.stringPropertyNames()) {
//                taskConfig.put(name, p.getProperty(name));
//            }
//        }
//        return taskConfig.get(key);
//    }
//
//    public static boolean getTaskConfigBool(String key) throws IOException {
//        String result = getTaskConfigString(key);
//        if ("true".equals(result)) return true;
//        return false;
//    }
//
//    public static int getTaskConfigInt(String key) throws Exception {
//        String result = getTaskConfigString(key);
//        return Integer.parseInt(result);
//    }
}
