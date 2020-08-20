package com.qcm.graph;

public class ArangoBusinessCompanyLPInfo {
    public String name;
    public String v_id;
    public String e_id;

    public ArangoBusinessCompanyLPInfo(String name, String v_id, String e_id) {
        this.name = name;
        this.v_id = v_id;
        this.e_id = e_id;
    }

    public ArangoBusinessCompanyLPInfo(String name) {
        this(name, null, null);
    }
}
