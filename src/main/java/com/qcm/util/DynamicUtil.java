package com.qcm.util;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class DynamicUtil {
    public static boolean loadJar(String path) {
        File jar = new File(path);
        if (jar.exists()) {
            Method method = null;
            try {
                method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            } catch (NoSuchMethodException | SecurityException e) {
                e.printStackTrace();
                return false;
            }

            boolean accessible = method.isAccessible();
            try {
                if (!accessible) method.setAccessible(true);

                URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();

                URL url = jar.toURI().toURL();

                method.invoke(classLoader, url);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            } finally {
                method.setAccessible(accessible);
            }
            return true;
        }
        return false;
    }

    public static boolean loadClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
//            Object inst = clazz.newInstance();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void testLoadJar() {
        System.out.println("=========class path: "+System.getProperty("java.class.path"));
        boolean r1 = DynamicUtil.loadJar("elasticsearch-7.6.0.jar");
        if (r1) {
            System.out.println("=========== successed to load elasticsearch-7.6.0.jar");
            boolean r2 = DynamicUtil.loadClass("org.elasticsearch.index.query.QueryBuilder");
            System.out.println(String.format("========= load QueryBuilder: %b", r2));
        } else {
            System.out.println("======== failed to load jar: elasticsearch-7.6.0.jar");
        }
    }
}
