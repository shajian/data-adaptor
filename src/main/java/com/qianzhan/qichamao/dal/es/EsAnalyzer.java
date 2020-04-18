package com.qianzhan.qichamao.dal.es;

public enum EsAnalyzer {
    standard("std"),
    hanlp_index("index"),
    hanlp_crf("crf"),
    hanlp_nlp("nlp"),
    hanlp("smart"),
    sep_analyzer("sep"),
    keyword("key");

    private final String f_name;
    private EsAnalyzer() { this.f_name = null; }
    private EsAnalyzer(String name) {
        this.f_name = name;
    }

    public String getF_name() {
        if (this.f_name == null)
            return name();
        return this.f_name;
    }
}
