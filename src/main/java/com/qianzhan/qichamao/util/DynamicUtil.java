package com.qianzhan.qichamao.util;

import org.bson.types.ObjectId;

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
}
