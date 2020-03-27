package com.qianzhan.qichamao.dal.hbase;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseTableMeta {
    String table_name();
    String[] families();
    int[] max_versions();
}
