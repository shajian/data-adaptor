package com.qianzhan.qichamao.graph;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ArangoCollectionMetas {
    ArangoCollectionMeta[] value();
}