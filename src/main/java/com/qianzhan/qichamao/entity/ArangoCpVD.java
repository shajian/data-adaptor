package com.qianzhan.qichamao.entity;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentField;
import com.qianzhan.qichamao.util.Cryptor;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;

import java.text.MessageFormat;
import java.util.Map;

@Getter
public class ArangoCpVD {
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
    /**
     * 1: company
     * 2: person
     */
    private int type;

    private ArangoCpVD() { }

    /**
     * only for company vertex
     * @param code
     * @param name
     * @param area
     */
    public ArangoCpVD(String code, String name, String area) {
        type = 1;
        this.name = name;
        this.area = area == null ? "" : area;
        this.key = code;
    }

    /**
     * only for person vertex or company vertex with no code
     * @param name
     * @param code for type=1, this vertex is company, but the `code` is not belongs this vertex,
     *             instead, `code` is it's related company vertex. e.g. company A has a company-type legal person,
     *             which is also a company(B), when store B as a vertex, if B has no vertex, A's code must be
     *             provided as this parameter.
     * @param type
     */
    public ArangoCpVD(String name, String code, int type) {
        this.type = type;
        this.name = name;
        this.key = String.format("%s%s", code, Cryptor.md5(name));
    }

    public BaseDocument to() {
        BaseDocument doc = new BaseDocument(this.key);
        doc.addAttribute("name", name);
        doc.addAttribute("type", type);
        if (type == 1) {
            doc.addAttribute("area", area);
        }
        return doc;
    }

    public static ArangoCpVD from(BaseDocument doc) {
        ArangoCpVD v = new ArangoCpVD();
        v.key = doc.getKey();
        v.id = doc.getId();
        Map<String, Object> props = doc.getProperties();
        if (props.containsKey("type")) {
            v.type = (Integer) props.get("type");
        }
        v.area = (String) props.get("area");
        v.name = (String) props.get("name");
        return v;
    }

    /**
     * convert this instance to `upsert` AQL
     * update: `key` and `type` cannot be updated
     * @param coll the collection name
     * @return
     */
    public String upsertAql(String coll) {
        String aql = "UPSERT { _key: '%s' } INSERT { _key: '%s', name: '%s', type: %d";
        if (type == 2) {    // person, no need to store `area`
            aql += " } UPDATE { name: '%s' } IN %s OPTIONS { keepNull: false }";
            aql = String.format(aql, key, key, name, type, name, coll);
        } else {    // company
            aql += ", area: '%s' } UPDATE { name: '%s', area: '%s' } IN %s OPTIONS { keepNull: false }";
            aql = String.format(aql, key, key, name, type, area, name, area, coll);
        }
        // if want to return old value, please append
        // aql += " RETURN { doc: NEW, type: OLD ? 'update' : 'insert' }";
        return aql;
    }
}
