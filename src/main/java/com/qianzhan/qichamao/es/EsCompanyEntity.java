package com.qianzhan.qichamao.es;

import com.qianzhan.qichamao.entity.OrgCompanyList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.common.geo.GeoPoint;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * support
 *  1. aggregation?
 *  2. filter?
 *  3.
 */
@Setter
@Getter
@ToString
@EsIndexMeta(index = "company_2", id = "oc_code")
public class EsCompanyEntity {
    /**
     * 1. type=="text", if analyzers.length==1,
     */


    @EsFieldMeta
    private String oc_code;     // doc_values=true to enable search-after function
    @EsFieldMeta(type= EsFieldType.text, analyzers = {EsAnalyzer.hanlp_index, EsAnalyzer.hanlp, EsAnalyzer.keyword})
//    @EsFieldMeta(type=EsFieldType.text)
    private String oc_name;
    @EsFieldMeta(type=EsFieldType.text)
    private String oc_area;
    @EsFieldMeta
    private String m_area;      // aggregation
    @EsFieldMeta(doc_values = false)
    private List<String> mobile_phones;
    @EsFieldMeta(doc_values = false)
    private List<String> fix_phones;
    @EsFieldMeta(doc_values = false)
    private List<String> mails;
    @EsFieldMeta(type=EsFieldType.text)
    private List<String> gb_codes;
    @EsFieldMeta
    private List<String> gb_cats;
    @EsFieldMeta(type = EsFieldType.geo_point)
    private GeoPoint coordinate;
    @EsFieldMeta(type = EsFieldType.Byte)
    private byte oc_status;
    @EsFieldMeta(doc_values = false)
    private String legal_person;
    @EsFieldMeta(doc_values = false)
    private List<String> senior_managers;
    @EsFieldMeta(doc_values = false)
    private List<String> share_holders;
    @EsFieldMeta(type = EsFieldType.text, analyzers = {EsAnalyzer.hanlp_index, EsAnalyzer.hanlp, EsAnalyzer.keyword})
//    @EsFieldMeta(type = EsFieldType.text)
    private List<String> brands;
    @EsFieldMeta(doc_values = false)
    private List<String> old_names;
    @EsFieldMeta(type = EsFieldType.text, analyzers = {EsAnalyzer.hanlp_index, EsAnalyzer.hanlp})
//    @EsFieldMeta(type = EsFieldType.text)
    private String business;
    @EsFieldMeta(type = EsFieldType.text, analyzers = {EsAnalyzer.hanlp_index, EsAnalyzer.hanlp})
//    @EsFieldMeta(type = EsFieldType.text)
    private String oc_address;
    @EsFieldMeta(type = EsFieldType.Double)
    private double register_money;
    @EsFieldMeta(type = EsFieldType.date)
    private Date establish_date;
    @EsFieldMeta(doc_values = false)
    private List<String> oc_types;

//    /**
//     * concatenate all other dimensions with boolean values with separator '|'
//     *
//     * but, how to do with those dimensions with numerical or enumerated values?
//     * 1. ignore them
//     * 2. list them one by one as fields
//     */
//    @EsFieldMeta(doc_values = false, type = EsFieldType.text, analyzers = {EsAnalyzer.sep_analyzer})
//    private String ext_dims;

    /**
     * artificial labeling
     */
    @EsFieldMeta(doc_values = false)
    private List<String> tags;      // unweighted tags
    @EsFieldMeta(doc_values = false)
    private String tag_1;
    @EsFieldMeta(doc_values = false)
    private String tag_2;
    @EsFieldMeta(doc_values = false)
    private String tag_3;
    @EsFieldMeta(type = EsFieldType.integer)
    private int score_1;
    @EsFieldMeta(type = EsFieldType.integer)
    private int score_2;
    @EsFieldMeta(type = EsFieldType.integer)
    private int score_3;

    /**
     * comprehend score for each document
     */
    @EsFieldMeta(type = EsFieldType.integer)
    private int score;
    @EsFieldMeta(type=EsFieldType.Double)
    private double weight;

    public void loadFrom(OrgCompanyList c) {
        this.oc_name = c.oc_name;
        this.oc_area = c.oc_area;
        this.oc_address = c.oc_address;
        this.oc_code = c.oc_code;
        if (c.oc_area.length() >= 2) {
            this.m_area = c.oc_area.substring(0, 2);
        } else {
            this.m_area = "--";
            this.oc_area = "--";
        }
        this.oc_types = new ArrayList<>(2);
        if (c.equals("个体工商户") || c.equals("个体")) {
            this.oc_types.add("个体工商户");
        }

        this.establish_date = c.oc_issuetime;
    }


}
