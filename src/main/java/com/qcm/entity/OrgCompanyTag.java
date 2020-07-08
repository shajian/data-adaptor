package com.qcm.entity;

import java.util.Comparator;

public class OrgCompanyTag {
    public String code;
    public String brandname;
    public int score;
    public boolean isvalid;

    public static Comparator<OrgCompanyTag> comparator = new Comparator<OrgCompanyTag>() {
        @Override
        public int compare(OrgCompanyTag o1, OrgCompanyTag o2) {
            if (o1.score < o2.score) return -1;
            if (o1.score > o2.score) return 1;
            return 0;
        }
    };
}
