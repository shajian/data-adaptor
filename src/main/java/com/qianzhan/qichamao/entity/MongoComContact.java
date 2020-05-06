package com.qianzhan.qichamao.entity;

import java.util.List;

public class MongoComContact {
    public String _id;  // code+type+contact

    public String code; // company oc_code
    public String contact;
    // QQ, mail, fix_phone, mobile_phone
    public byte type;

}
