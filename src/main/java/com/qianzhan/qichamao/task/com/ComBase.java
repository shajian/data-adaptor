package com.qianzhan.qichamao.task.com;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.Callable;

/**
 * Base class, acts as a data pack.
 * Each subclass inherited this base class fills a special part of fields of
 * a company.
 */
@Getter@Setter
public abstract class ComBase implements Callable<Boolean> {
    protected ComPack compack;
    protected String tasks_key;

    public ComBase(ComPack cp) { compack = cp; }

    public ComBase(String tasks_key) {
        this.tasks_key = tasks_key;
        compack = SharedData.get(tasks_key);
    }
}
