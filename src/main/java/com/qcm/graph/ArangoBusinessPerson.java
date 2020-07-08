package com.qcm.graph;

import com.arangodb.entity.BaseDocument;
import com.qcm.util.Cryptor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ArangoBusinessPerson {


    private String id;
    private String key; // composed with related company code and md5(person name)
    private String name;    // person name
    private long degree;    // outgoing
    private ArangoBusinessPerson() { }
    public ArangoBusinessPerson(String name, String code) {
        this.name = name;
        this.key = code + Cryptor.md5(name);
        this.id = String.format("%s/%s", collection, key);
    }
    public static String collection;
    public static String toId(String key) {
        return String.format("%s/%s", collection, key);
    }

    public BaseDocument to() {
        BaseDocument doc = new BaseDocument();
        doc.setKey(this.key);
        doc.setId(this.id);
        doc.addAttribute("name", this.name);
        doc.addAttribute("degree", this.degree);
        return doc;
    }

    public static ArangoBusinessPerson from(BaseDocument doc) {
        ArangoBusinessPerson p = new ArangoBusinessPerson();
        p.id = doc.getId();
        p.key = doc.getKey();
        p.name = (String) doc.getAttribute("name");
        p.degree = (Long) doc.getAttribute("degree");
        return p;
    }
}
