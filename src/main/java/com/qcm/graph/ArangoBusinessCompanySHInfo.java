package com.qcm.graph;

import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.util.MiscellanyUtil;

import java.util.HashMap;
import java.util.Map;

public class ArangoBusinessCompanySHInfo {
    public String name;
    public String v_id;
    public String e_id;
    public Double money;
    public Float ratio;
    public boolean keep;        // in terms of old vertex

    public ArangoBusinessCompanySHInfo(String name, String v_id, String e_id) {
        this.name = name;
        this.v_id = v_id;
        this.e_id = e_id;
        money = 0.;
    }

    public ArangoBusinessCompanySHInfo(String name) {
        this(name, null, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArangoBusinessCompanySHInfo) {
            ArangoBusinessCompanySHInfo that = (ArangoBusinessCompanySHInfo) obj;
            if (!MiscellanyUtil.equals(this.name, that.name)) return false;
            if (money == null && that.money == null) return true;
            if (money != null && that.money != null && money == that.money && ratio == that.ratio) return true;
        }
        return false;
    }

    public BaseEdgeDocument toEdge(String toId) {
        BaseEdgeDocument edge = new BaseEdgeDocument();
        edge.setId(e_id);
        edge.setFrom(v_id);
        edge.setTo(toId);
        Map<String, Object> props = new HashMap<>();
        props.put("type", 2);
        if (money != null) {
            props.put("money", money);
            props.put("ratio", ratio);
        }
        edge.setProperties(props);
        return edge;
    }
}
