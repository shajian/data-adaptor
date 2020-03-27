package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

@Getter@Setter
public class HbaseVersions<T extends IHbaseSerializable<T>> {
    private HbaseVersion<T>[] values;
    private HbaseDiffVersion<T>[] diffs;

    /**
     * use ASC as the default order in terms of version(timestamp).
     * this is because multi-versions is used to store historical
     * values of some field, ASC is very human-friend to scan
     * variation with time going.
     */
    private SortOrder order = SortOrder.asc;

    public enum SortOrder {
        none,
        asc,
        desc
    }
}
