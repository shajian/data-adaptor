package com.qianzhan.qichamao.graph;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.List;

public class ArangoGraphPath {
    public BaseDocument vertex;     // end vertex
    public BaseEdgeDocument edge;   // edge connects to end vertex

    // ==================== path ==================
    public List<BaseDocument> vertices;     // from start_vertex to end_vertex
    // count of edges are 1 less than count of vertices
    public List<BaseEdgeDocument> edges;

    public String endId() {
        if (vertex != null) return vertex.getId();
        if (edge != null) return edge.getTo();
        if (!MiscellanyUtil.isArrayEmpty(vertices)) return vertices.get(vertices.size() - 1).getId();
        if (!MiscellanyUtil.isArrayEmpty(edges)) return edges.get(edges.size() - 1).getTo();
        return null;
    }
    public String secondaryEndId() {
        if (edge != null) return edge.getFrom();
        if (vertices != null && vertices.size() > 1) return vertices.get(vertices.size() - 2).getId();
        if (!MiscellanyUtil.isArrayEmpty(edges)) return edges.get(edges.size() - 1).getFrom();
        return null;
    }
}
