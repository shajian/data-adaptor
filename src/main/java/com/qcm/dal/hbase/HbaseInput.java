package com.qcm.dal.hbase;

import lombok.Getter;
import lombok.Setter;

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
