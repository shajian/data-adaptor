package com.qcm.graph;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(ArangoGraphMetas.class)
public @interface ArangoGraphMeta {
    byte env() default 1;
    String db();
    String graph();
    String edge();
    String[] froms();
    String[] tos();
}
