package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ArangoCpED {
    private String id;
    private String key;
    private String from;
    private String to;



    /**
     * Use multi edges to represent multi-relations between `from` and `to`
     */

    /** [from] [type] [to]
     * 1. act as legal person of
     * 2. invest
     * 3. occupy a position in
     */
    private int type;

    /**
     * money of share sholder `from` invested to `to` company
     */
    private double money;
//    /**
//     * rate= money / [total money of share holders of `to` company]
//     */
//    private float money_rate;

    private String position;

    public ArangoCpED(String from, String to, String key, int type) {
        this.from = from;
        this.to = to;
        this.key = key;
        this.type = type;
    }
}
