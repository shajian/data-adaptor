package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.task.stat.BrowseCount;
import com.qianzhan.qichamao.task.stat.CompanyStatisticsInfo;

public class EsComPreHook {

    public static void start() throws Exception {
        CompanyStatisticsInfo.initialize();
        BrowseCount.initialize();
    }
}
