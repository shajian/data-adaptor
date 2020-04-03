package com.qianzhan.qichamao.dal.es;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EsIndexMeta {
    String index();
    String id();
    String type() default "_doc";
}
