package com.qianzhan.qichamao.dal.es;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EsFieldMeta {
    EsFieldType type() default EsFieldType.keyword;

    /**
     * for keyword/numeric/date type field, if not used as
     *  aggregation/sort/access from script, set it's
     *  doc_values to false to save disk space
     * @return
     */
    boolean doc_values() default true;
    /**
     * all analyzer must be sorted ascend
     * @return
     */
    EsAnalyzer[] analyzers() default {EsAnalyzer.standard};
}
