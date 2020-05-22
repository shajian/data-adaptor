package com.qianzhan.qichamao.entity;

import java.util.List;

public class MongoPersonAgg {
    /** person's name and its directly related company's code make the document unique */
    // person name
    // this field should be indexed
    public String name;
    // oc_code of company 1
    public String code1;                // index
    // oc_code of company 2
    public String code2;                // index
    /**
     * 1: name of company 1 and name of company 2 are the same person
     * 2: name of company 1 and name of company 2 are not same person
     * 0: the indication is invalid, and nobody know whether they are the same person.
     *      when status=0, this document can be removed from Mongodb
     */
    public byte status;

    public List<String> code1s;
    public List<String> code2s;

    // mongodb id
    public String _id;  // composed by code1+code2+name

}
