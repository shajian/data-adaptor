package com.qianzhan.qichamao.entity;

import com.qianzhan.qichamao.dal.es.EsAnalyzer;
import com.qianzhan.qichamao.dal.es.EsFieldMeta;
import com.qianzhan.qichamao.dal.es.EsFieldType;
import com.qianzhan.qichamao.dal.es.EsIndexMeta;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;

/**
 * Just for statistics of `COMPANY`.
 * Provides many kinds of statistics, but only one kind of searching which
 *  is executed on company name. This ES index's aim is not the same as that of `EsCompany`,
 *  which is used mainly for searching and supports part of statistics/aggregations extra.
 */
@EsIndexMeta(index = "comstat_2", id = "oc_code")
@Getter@Setter
public class EsComStat {
    @EsFieldMeta
    private String oc_code;
    @EsFieldMeta(type= EsFieldType.text, analyzers =
            {EsAnalyzer.hanlp_index, EsAnalyzer.hanlp_crf, EsAnalyzer.hanlp_nlp, EsAnalyzer.keyword})
    private String oc_name;
    @EsFieldMeta(type = EsFieldType.Byte)
    private byte oc_type;
    @EsFieldMeta(type = EsFieldType.Byte)
    private byte oc_status;
    @EsFieldMeta(type = EsFieldType.geo_point)
    private EsGeoPoint coordinate;
    @EsFieldMeta
    private String area_2;
    @EsFieldMeta
    private String area_4;
    @EsFieldMeta(type = EsFieldType.date)
    private Date establish_date;
    @EsFieldMeta(type = EsFieldType.Double)
    private double register_money;
    @EsFieldMeta
    private String money_type;

    @EsFieldMeta
    private List<String> gb_cats;
    @EsFieldMeta
    private String gb_cat;
//    private List<String> gb_codes;
    @EsFieldMeta
    private String tax;
    @EsFieldMeta
    private int listed;
    @EsFieldMeta(type = EsFieldType.bool)
    private boolean mail;
    @EsFieldMeta(type = EsFieldType.bool)
    private boolean mobile_phone;
    @EsFieldMeta(type = EsFieldType.bool)
    private boolean fix_phone;

    @EsFieldMeta(type = EsFieldType.integer)
    private int brand;
    @EsFieldMeta(type = EsFieldType.integer)
    private int patent;
    @EsFieldMeta(type = EsFieldType.integer)
    private int finance;
    @EsFieldMeta(type = EsFieldType.integer)
    private int tender_bid;
    @EsFieldMeta(type = EsFieldType.integer)
    private int im_ex_port;
    @EsFieldMeta(type = EsFieldType.integer)
    private int insurance;
    @EsFieldMeta(type = EsFieldType.integer)
    private int domain;
    @EsFieldMeta(type = EsFieldType.integer)
    private int notice;
    @EsFieldMeta(type = EsFieldType.integer)
    private int execution;
    @EsFieldMeta(type = EsFieldType.integer)
    private int judge;
    @EsFieldMeta(type = EsFieldType.integer)
    private int dishonor;
    @EsFieldMeta(type = EsFieldType.integer)
    private int product;
    @EsFieldMeta(type = EsFieldType.integer)
    private int software;

}
