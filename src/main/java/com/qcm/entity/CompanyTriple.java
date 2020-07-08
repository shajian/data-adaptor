package com.qcm.entity;

public class CompanyTriple {
    public String oc_name;
    public String oc_area;
    public String oc_code;

    public CompanyTriple() {}
    public CompanyTriple(String code, String name, String area) {
        oc_code = code;
        oc_name = name;
        oc_area = area;
    }
}
