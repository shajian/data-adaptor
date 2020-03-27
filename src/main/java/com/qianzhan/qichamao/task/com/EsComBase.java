package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.MongoCompany;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

@Getter@Setter
public abstract class EsComBase implements Callable<Boolean> {
    private EsCompany company;
    private EsComStat comstat;
    private MongoCompany es_complement;
    private CountDownLatch latch;

    public void init(EsCompany c, MongoCompany es_c) {
        init(c, es_c, null);
    }

    public void init(EsComStat s) {
        init(null, null, s);
    }

    public void init(EsCompany c, MongoCompany es_c, EsComStat s) {
        company = c;
        es_complement = es_c;
        comstat = s;
    }
}
