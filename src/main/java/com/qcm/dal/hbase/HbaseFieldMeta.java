package com.qcm.dal.hbase;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HbaseFieldMeta {
    String family();
    String type();
}
