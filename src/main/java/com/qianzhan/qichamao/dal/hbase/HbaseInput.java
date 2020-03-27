package com.qianzhan.qichamao.dal.hbase;

import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter@Setter
public class HbaseInput<T> {

    private String[] keys;
    private GetMode getMode = GetMode.get;

    private String[] families;

    private long version;
    private long max_version;

    public enum GetMode {
        get,
        scan
    }
}
