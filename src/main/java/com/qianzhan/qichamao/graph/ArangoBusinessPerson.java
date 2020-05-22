package com.qianzhan.qichamao.graph;

import com.qianzhan.qichamao.util.Cryptor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ArangoBusinessPerson {


    private String id;
    private String key; // composed with related company code and md5(person name)
    private String name;    // person name
    private long degree;    // outgoing

    public ArangoBusinessPerson(String name, String code) {
        this.name = name;
        this.key = code + Cryptor.md5(name);
        this.id = String.format("%s/%s", collection, key);
    }
    public static String collection;
    public static String toId(String key) {
        return String.format("%s/%s", collection, key);
    }
}
