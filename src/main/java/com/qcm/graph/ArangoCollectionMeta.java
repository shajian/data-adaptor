package com.qcm.graph;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(ArangoCollectionMetas.class)
public @interface ArangoCollectionMeta {
    byte env() default 1;
    String db();
    String collection();
    // each index follows the form of "<field>[.u][.s]". for simplicity, we do not consider compound index
    //  which usually composed by multiply fields.
    //  where u means "unique" while s means "sparse"
    String[] indices() default {};
}
