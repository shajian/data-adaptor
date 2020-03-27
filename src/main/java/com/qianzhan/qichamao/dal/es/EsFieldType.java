package com.qianzhan.qichamao.dal.es;

public enum EsFieldType {
    keyword,
    text,
    date,
    geo_point,
    Byte("byte"),
    Short("short"),
    integer,
    bool("boolean"),
    Long("long"),
    Double("double"),
    Float("float");

    private final String f_name;
    private EsFieldType() {
        this.f_name = null;
    }
    private EsFieldType(String name) {
        this.f_name = name;
    }

    public String getF_name() {
        if (this.f_name != null) return this.f_name;
        return name();
    }
}
