package com.qcm.graph;

import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.util.MiscellanyUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ArangoBusinessCompanySMInfo {
    public String name;
    public String v_id;
    public String e_id;
    public Set<String> occupations;

    public ArangoBusinessCompanySMInfo(String name, String v_id, String e_id) {
        this.name = name;
        this.v_id = v_id;
        this.e_id = e_id;
        occupations = new HashSet<>();
    }

    public ArangoBusinessCompanySMInfo(String name) {
        this(name, null,null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArangoBusinessCompanySMInfo) {
            ArangoBusinessCompanySMInfo that = (ArangoBusinessCompanySMInfo) obj;
            if (!MiscellanyUtil.equals(this.name, that.name)) return false;
            if (occupations == null && that.occupations == null) return true;
            if (occupations != null && that.occupations != null) {
                that.occupations.removeAll(occupations);
                if (that.occupations.size() == 0)
                    return true;
            }
        }
        return false;
    }

    public BaseEdgeDocument toEdge(String toId) {
        BaseEdgeDocument edge = new BaseEdgeDocument();
        edge.setId(e_id);
        edge.setFrom(v_id);
        edge.setTo(toId);
//        edge.setKey(e_id.split("/")[1]);
        Map<String, Object> props = new HashMap<>();
        props.put("type", 3);
        if (occupations.size()>0) {
            props.put("position", String.join(",", occupations));
        }
        edge.setProperties(props);
        return edge;
    }
}
