package com.qianzhan.qichamao.entity;

@Deprecated
public class ArangoCpIdVD {
    public String id;

    // for company-type vertex, key is oc_code
    // for person-type vertex, key is no+md5(name)
    public String key;

    // index, sequence number
    public long sq;
    // index
    public String name;
}
