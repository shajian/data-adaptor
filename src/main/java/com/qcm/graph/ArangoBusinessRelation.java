package com.qcm.graph;

import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.util.MiscellanyUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ArangoBusinessRelation {
    public static String collection;
    private String id;
    private String key;
    private String from;
    private String to;
//    // if company A has a share holder which is company B,
//    // but B has a name which is shared by multi companies,
//    // and then we can't make sure which company is the true share holder of A,
//    // so set `share` to true, means that those companies are all share holders of A temporarily.
//    private boolean share;

    /**
     * Use multi edges to represent multi-relations between `from` and `to`
     */

    /** [from] [type] [to]
     * 1. act as legal person of
     * 2. invest
     * 3. occupy a position in
     * 4. has a same contact with(deprecated)
     */
    private long type;

    /**
     * money of share sholder `from` invested to `to` company
     */
    private double money;
    /**
     * rate= money / [total money of share holders of `to` company]
     */
    private float ratio;

    private String position;

    private ArangoBusinessRelation() { }
    public ArangoBusinessRelation(String from, String to, String key, int type) {
        this.from = from;
        this.to = to;
        this.key = key;
        this.type = type;
        this.id = String.format("%s/%s", collection, this.key);
//        this.share = share;
    }

    public static ArangoBusinessRelation from(BaseEdgeDocument doc) {
        ArangoBusinessRelation e = new ArangoBusinessRelation();
        e.id = doc.getId();
        e.key = doc.getKey();
        e.from = doc.getFrom();
        e.to = doc.getTo();
        Map<String, Object> props = doc.getProperties();
        e.type = (Long) props.get("type");

//        Object share = props.get("share");
//        if (share != null) {
//            e.share = (Boolean) share;
//        }
        Object money = props.get("money");
        if (money != null) {
            e.money = (Double) money;
        }
        Object ratio = props.get("ratio");
        if (ratio != null) {
            double r = (Double) ratio;
            e.ratio = (float) r;
        }
        e.position = (String) props.get("position");
        return e;
    }
    public static List<ArangoBusinessRelation> from(List<BaseEdgeDocument> docs) {
        if (MiscellanyUtil.isArrayEmpty(docs)) return null;
        List<ArangoBusinessRelation> eds = new ArrayList<>();
        for (BaseEdgeDocument doc : docs) {
            if (doc == null) continue;
            eds.add(ArangoBusinessRelation.from(doc));
        }
        return eds;
    }

    public BaseEdgeDocument to() {
        BaseEdgeDocument doc = new BaseEdgeDocument(this.key, this.from, this.to);
        doc.setId(this.id);
        doc.addAttribute("type", type);
//        if (share)
//            doc.addAttribute("share", share);
        if (type == 2) {
            // share holder
            doc.addAttribute("money", money);
            doc.addAttribute("ratio", ratio);
        } else if (type == 3) {     // senior member
            doc.addAttribute("position", position);
        }
        return doc;
    }
}
