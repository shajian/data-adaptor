package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MongoComContact {
    public String _id;  // code+type+contact

    public String code; // company oc_code
    public String contact;
    // QQ, mail, fix_phone, mobile_phone
    public byte type;

}
