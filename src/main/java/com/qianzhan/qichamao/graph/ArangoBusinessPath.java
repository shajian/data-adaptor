package com.qianzhan.qichamao.graph;

import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.List;

public class ArangoBusinessPath {
    public ArangoBusinessVertex vertex;     // end vertex
    public ArangoBusinessEdge edge;   // edge connects to end vertex

    // ==================== path ==================
    public List<ArangoBusinessVertex> vertices;     // from start_vertex to end_vertex
    // count of edges are 1 less than count of vertices
    public List<ArangoBusinessEdge> edges;

    public String endId() {
        if (vertex != null) return vertex._id;
        if (edge != null) return edge._to;
        if (!MiscellanyUtil.isArrayEmpty(vertices)) return vertices.get(vertices.size() - 1)._id;
        if (!MiscellanyUtil.isArrayEmpty(edges)) return edges.get(edges.size() - 1)._to;
        return null;
    }
    public String secondaryEndId() {
        if (edge != null) return edge._from;
        if (vertices != null && vertices.size() > 1) return vertices.get(vertices.size() - 2)._id;
        if (!MiscellanyUtil.isArrayEmpty(edges)) return edges.get(edges.size() - 1)._from;
        return null;
    }
}
