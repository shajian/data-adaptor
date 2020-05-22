package com.qianzhan.qichamao.dal.arangodb;

import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import lombok.Getter;
import lombok.Setter;

@Getter@Setter
public class ArangoComInput {
    // graph traverse searching
    private String start_id;
    private String end_id;
    private boolean skipShare;
    private int minDepth;
    private int maxDepth;

    private String filter = "";
    private String prune = "";
    private String return_;

    public ArangoComInput(String start_id, int maxDepth) {
        this(start_id, 1, maxDepth);
    }

    public ArangoComInput(String start_id, int minDepth, int maxDepth) {
        if (!start_id.contains("/")) throw new ValueException("start_id must contain '/'");
        this.start_id = start_id;
        if (minDepth < 1) minDepth = 1;
        if (maxDepth < minDepth) maxDepth = minDepth;
        this.minDepth = minDepth;
        this.maxDepth = maxDepth;
    }

    public ArangoComInput(String start_id, String end_id) {
        if (!start_id.contains("/") || !end_id.contains("/"))
            throw new ValueException("start_id and end_id must contain '/'");
        this.start_id = start_id;
        this.end_id = end_id;
    }
}
