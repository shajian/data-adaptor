package com.qcm.graph;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ArangoGraphMetas {
    ArangoGraphMeta[] value();
}
