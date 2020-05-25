package com.qianzhan.qichamao.graph;

import com.qianzhan.qichamao.entity.CompanyTriple;

import java.util.ArrayList;
import java.util.List;

public class PersonAggregation {
    // there exists unknown company which only have a name(but no code or area)
    // for unknowm company, its code equals to md5(name)
    // if companies.size() < lps.size() + shs.size() + sms.size(),
    //      it means some companies are hided, and you must click button 'MORE' to show all.
    public List<CompanyTriple> companies;
    // persons who this one knows
    public List<String> persons;

    // codes of companies which are related with this one
    //
    // should be noted that when there are very a lot of related companies, these
    // fields may not cover all, when searching for list data.
    public List<String> lps;
    public List<String> shs;
    public List<String> sms;         // code of companies which have sm roles the person acts

    // person id of center of this group
    public String person;

    public PersonAggregation() {
        this.companies = new ArrayList<>();
        this.persons = new ArrayList<>();
        this.lps = new ArrayList<>();
        this.shs = new ArrayList<>();
        this.sms = new ArrayList<>();
    }
}
