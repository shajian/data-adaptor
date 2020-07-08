package com.qcm.entity;

import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.util.MiscellanyUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Deprecated
public class ArangoCpED {
    private String id;
    private String key;
    private String from;
    private String to;
    // if company A has a share holder which is company B,
    // but B has a name which is shared by multi companies,
    // and then we can't make sure which company is the true share holder of A,
    // so set `share` to true, means that those companies are all share holders of A temporarily.
    private boolean share;

    /**
     * Use multi edges to represent multi-relations between `from` and `to`
     */

    /** [from] [type] [to]
     * 1. act as legal person of
     * 2. invest
     * 3. occupy a position in
     * 4. has a same contact with
     */
    private int type;
    /** distance */
    private int dist=1;

    /**
     * money of share sholder `from` invested to `to` company
     */
    private double money;
    /**
     * rate= money / [total money of share holders of `to` company]
     */
    private float ratio;

    private String position;

    private ArangoCpED() { }
    public ArangoCpED(String from, String to, String key, int type, boolean share) {
        this.from = from;
        this.to = to;
        this.key = key;
        this.type = type;
        this.share = share;
    }

    public static ArangoCpED from(BaseEdgeDocument doc) {
        ArangoCpED e = new ArangoCpED();
        e.id = doc.getId();
        e.key = doc.getKey();
        e.from = doc.getFrom();
        e.to = doc.getTo();
        Map<String, Object> props = doc.getProperties();
        long type = (Long) props.get("type");
        e.type = (int) type;
        long dist = (Long) props.get("dist");
        e.dist = (int) dist;
        Object share = props.get("share");
        if (share != null) {
            e.share = (Boolean) share;
        }
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
    public static List<ArangoCpED> from(List<BaseEdgeDocument> docs) {
        if (MiscellanyUtil.isArrayEmpty(docs)) return null;
        List<ArangoCpED> eds = new ArrayList<>();
        for (BaseEdgeDocument doc : docs) {
            if (doc == null) continue;
            eds.add(ArangoCpED.from(doc));
        }
        return eds;
    }

    public BaseEdgeDocument to() {
        BaseEdgeDocument doc = new BaseEdgeDocument(this.key, this.from, this.to);
        doc.addAttribute("type", type);
        doc.addAttribute("dist", dist);
        if (share)
            doc.addAttribute("share", share);
        if (type == 2) {
            // share holder
            doc.addAttribute("money", money);
            doc.addAttribute("ratio", ratio);
        } else if (type == 3) {     // senior member
            doc.addAttribute("position", position);
        }
        return doc;
    }

    /**
     * convert this instance to `insert` AQL
     * `key` `from` `to` and `type` can not be updated
     * @param coll collection name
     * @return
     */
    public String upsertAql(String coll) {
        String aql = "UPSERT { _key: '%s' } INSERT { _key: '%s', _from: '%s', _to: '%s', dist: %d, type: %d";
        String share_s = share ? "true" : "null";
        if (share) {
            aql += ", share: true";
        }
        if (type == 1) {
            aql += " } UPDATE { dist: %d, share: %s } IN %s OPTIONS { keepNull: false }";
            aql = String.format(aql, key, key, from, to, type, share_s, coll);
        }
        if (type == 2) {
            aql += ", money: %.4f, ratio: %.4f } UPDATE { dist: %d, money: %.4f, ratio: %.4f, share: %s } IN %s OPTIONS { keepNull: false }";
            aql = String.format(aql, key, key, from, to, type, money, ratio, money, ratio, share_s, coll);
        } else if (type == 3) {
            aql += ", position: '%s' } UPDATE { dist: %d, position: '%s', share: %s } IN %s OPTIONS { keepNull: false }";
            aql = String.format(aql, key, key, from, to, type, position, coll);
        }
        // if want to return old value, please append
        // aql += " RETURN { doc: NEW, type: OLD ? 'update' : 'insert' }";
        return aql;
    }
}
