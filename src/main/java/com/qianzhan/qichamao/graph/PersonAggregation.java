package com.qianzhan.qichamao.graph;

import com.qianzhan.qichamao.entity.CompanyTriple;

import java.util.List;

public class PersonAggregation {
    public List<CompanyTriple> companies;
    public List<String> persons;
    public int lps;
    public int shs;
    public int sms;         // number of sm roles the person acts
    public int total;       // total number of roles this person acts
}
