package com.qianzhan.qichamao.app;

import com.qianzhan.qichamao.task.com.EsCompanyWriter;
import com.qianzhan.qichamao.util.DynamicUtil;

import java.io.IOException;

public class EsMongoWriter {
    public static void main(String[] args) {
        EsCompanyWriter.start();
        System.out.println("game f**king over");
    }

    private static void testLoadJar() {
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
