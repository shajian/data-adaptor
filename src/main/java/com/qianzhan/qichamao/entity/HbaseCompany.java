package com.qianzhan.qichamao.entity;

import com.qianzhan.qichamao.dal.hbase.HbaseTableMeta;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter@Setter
@HbaseTableMeta(table_name = "company", families = {"s", "m"}, max_versions = {1, 128})
public class HbaseCompany {
    //======================== family: s, timestamp: whenever ===========================
    private String oc_code;
    private String oc_number;
    private String oc_name;
    private String oc_area;
    private String credit_code;
    private Date establish_date;
    //======================== family: s, timestamp: whenever ============================

    //====================== family: s, timestamp: some meaningful ======================
    private HbaseVersion<HbaseString> legal_person;
    /**
     * make sure the max size is less then 5000
     */
    private HbaseVersion<HbaseComInvest> share_holders;
    /**
     * name,position...
     * make sure the max size is less then 5000
     */
    private HbaseVersion<HbaseComPosition> senior_managers;
    /**
     * make sure the max size is less then 5000
     */
    private HbaseVersion<HbaseComInvest> invests;
    //====================== family: s, timestamp: some meaningful ========================

    // ======================== family: m ===========================
    // x0+d0=x1
    // x1+d1=x2
    // x0+d0+d1+...+dn-1=xn
    private HbaseVersions<HbaseComInvest> diff_sh;
    private HbaseVersions<HbaseComPosition> diff_sm;
    private HbaseVersions<HbaseComInvest> diff_inv;
    // ======================== family: m ===========================




//    private final static int max_number = 5000;
//
//    public void setShare_holders(HbaseComInvest[] shs) {
//        if (shs.length <= max_number) {
//            share_holders = shs;
//        } else {
//            Arrays.sort(shs, ShareHolder.comparator);
//            // get top 4999+1
//            share_holders = new ShareHolder[max_number];
//            for (int i = 0; i < max_number-1; ++i) {
//                share_holders[i] = shs[shs.length -1 -i];
//            }
//            float value = 0;
//            float ratio = 0;
//            for (int i = max_number-1; i < shs.length; ++i) {
//                value += shs[i].value;
//            }
//            ShareHolder sh = new ShareHolder();
//            sh.setName("other");
//            sh.setValue(value);
//            share_holders[max_number-1] = sh;
//        }
//    }
//
//    public void setSenior_managers(String[] ms) {
//        if (ms.length <= max_number) {
//            senior_managers = ms;
//        } else {
//            senior_managers = new String[max_number];
//            for (int i = 0; i < max_number-1; ++i)
//                senior_managers[i] = ms[i];
//
//            senior_managers[max_number-1] = "others,-";
//        }
//    }
//
//    public void setInvests(Invest[] is) {
//        if (is.length <= max_number) {
//            invests = is;
//        } else {
//            Arrays.sort(is, Invest.comparator);
//            // get top 999+1
//            invests = new Invest[max_number];
//            for (int i = 0; i < max_number-1; ++i) {
//                invests[i] = is[is.length -1 -i];
//            }
//            float value = 0;
//            float ratio = 0;
//            for (int i = max_number-1; i < is.length; ++i) {
//                value += is[i].value;
//            }
//            Invest sh = new Invest();
//            sh.setName("other");
//            sh.setValue(value);
//            sh.setRatio(ratio);
//            invests[max_number-1] = sh;
//        }
//    }
//
}
