package com.qianzhan.qichamao.graph;

import com.arangodb.entity.BaseDocument;
import com.qianzhan.qichamao.util.Cryptor;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
public class ArangoBusinessCompany {

    /**
     * vertex collection name + "/" + key
     */
    private String id;

    /**
     * unique in vertex collection
     * for company vertex: key is code.
     * if a company has no code, e.g. company A has a share holder or legal person which is also a company B,
     * encoding B's name and prepend the A'scode, i.e. A-code + ":" + encode(B-name). What should be notice
     * is that the result string of encoded company name should not contain ':'.00000
     *
     * for natural person: as the same of above strategy.
     */
    private String key;
    private String name;
    private String area;
//    // used for sort
    private long degree;

    private ArangoBusinessCompany() { }

    /**
     * only for company vertex
     * @param code
     * @param name
     * @param area
     */
    public ArangoBusinessCompany(String code, String name, String area) {
        this.name = name;
        this.area = area;
        this.key = code;
        this.id = String.format("%s/%s", collection, code);
    }

    /**
     * only for person vertex or company vertex with no code
     * @param name
     */
    public ArangoBusinessCompany(String name) {
        this.name = name;
        if (MiscellanyUtil.isComposedWithAscii(name)) {
            name = name.replaceAll("\\s", "");
        }
        this.key = Cryptor.md5(name);
        this.id = String.format("%s/%s", collection, this.key);
    }

    public BaseDocument to() {
        BaseDocument doc = new BaseDocument(this.key);
        doc.setId(this.id);
        doc.addAttribute("name", name);
        doc.addAttribute("area", area);
        doc.addAttribute("degree", degree);
        return doc;
    }

    public boolean equals(BaseDocument doc) throws Exception {
        return equals(from(doc));
    }

    public boolean equals(ArangoBusinessCompany c) {
        if (this.id.equals(c.getId())) {
            return MiscellanyUtil.equals(this.name, c.name) && MiscellanyUtil.equals(this.area, c.name);
        }
        return false;
    }

    public static ArangoBusinessCompany from(BaseDocument doc) throws Exception {
        ArangoBusinessCompany v = new ArangoBusinessCompany();
        v.key = doc.getKey();
        v.id = doc.getId();
        if (!v.id.startsWith(collection)) {
            throw new Exception("document must in collection '" + collection
                    + "' but got from collection '"+ v.id.split("/")[0] + "'");
        }
        Map<String, Object> props = doc.getProperties();

        v.area = (String) props.get("area");
        v.name = (String) props.get("name");
        Long degree = (Long) props.get("degree");
        if (degree != null) v.degree = degree;
        return v;
    }

    public static List<ArangoBusinessCompany> from(List<BaseDocument> docs) throws Exception {
        if (MiscellanyUtil.isArrayEmpty(docs)) return null;
        List<ArangoBusinessCompany> vds = new ArrayList<>();
        for (BaseDocument doc : docs) {
            if (doc == null) continue;
            vds.add(ArangoBusinessCompany.from(doc));
        }
        return vds;
    }

    // just for env=1
    // for evolution, we will use env=2 and discard everything that related with env=1

    public static String collection;
    public static String toId(String key) {
        return String.format("%s/%s", collection, key);
    }
}
