package com.qianzhan.qichamao.dal.hbase;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HbaseFieldMeta {
    String family();
    String type();
}
