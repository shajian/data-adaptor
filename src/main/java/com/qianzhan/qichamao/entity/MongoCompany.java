package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter@Setter
public class MongoCompany {
    private String _id;
//    private String oc_code;
    private String oc_number;
    private String credit_code;
    private String oc_name;
    private String oc_area;

//    private Date establish_date;
    private String oc_logo;
    private String oc_money;

    public void loadFrom(OrgCompanyList c) {
        this._id = c.oc_code;
//        this.oc_code = c.oc_code;
        this.oc_name = c.oc_name;
        this.oc_number = c.oc_number;
        this.credit_code = c.oc_creditcode;
        this.oc_area = c.oc_area;
    }

    public void loadFrom(EsCompany c) {
        this._id = c.getOc_code();
//        this.oc_code = c.getOc_code();
        this.oc_name = c.getOc_name();
    }
}
