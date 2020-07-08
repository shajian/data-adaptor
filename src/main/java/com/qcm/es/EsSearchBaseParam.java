package com.qcm.es;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.search.sort.SortOrder;


@ToString
public class EsSearchBaseParam<T> {
//    @Getter@Setter
//    private String index; // this value is provided by EsBaseRepository

    /**
     * get by id
     * multi-get by ids
     */
    @Getter@Setter
    private String id;
    @Getter@Setter
    private String[] ids;
    @Getter@Setter
    private String field = "_id";


    /**
     * source filtering
     * fields from _source to retrieved
     * if set to true, use src_inc and src_exc; otherwise ignore those two fields
     */
    @Getter@Setter
    private boolean src_flag;
    @Getter
    private String[] src_inc;
    @Getter
    private String[] src_exc;

    public void setSrc_inc(String... inc) {
        if (inc.length > 0) {
            src_flag = true;
            src_inc = inc;
        }
    }

    public void setSrc_exc(String... exc) {
        if (exc.length > 0) {
            src_flag = true;
            src_exc = exc;
        }
    }

    @Getter@Setter
    private int from;   // starts from 0
    @Getter@Setter
    private int size = 10;

    @Getter@Setter
    private EsAggSetting aggs/* = EsAggSetting.getDefault("company.aggs")*/;
    /**
     * time_out for query. default to 8, unit is second
     */
    @Getter@Setter
    private int timeOut = 8;

    /**
     * field name to sort. field is belongs to T
     */
    @Getter@Setter
    private String sortField = "_score";
    @Getter@Setter
    private SortOrder sortOrder = SortOrder.DESC;

//    /**
//     * if supports page accessing randomly
//     * In normal cases, randomly accessing is off, and
//     * only authorized for vvip or administrator
//     */
//    private boolean randomAccess;
    /**
     * 0(default): from+size searching
     * 1: search after, used for paging down fast (cannot go backward)
     * 2: scroll, used for exporting mass of data
     * 3:
     */
    @Getter@Setter
    private byte accessMode;

    /**
     * if accessMode > 0, it needs to set scrollId
     */
    @Getter@Setter
    private String scrollId;
    @Getter@Setter
    private Object[] searchAfter;
    /**
     * fields to be highlighted
     */
    @Getter@Setter
    private String[] highlights;
    /**
     * // preTag and postTag, this field can be null or an array of two strings
     */
    @Getter@Setter
    private String[] highlightTags = new String[]{"<f**k>", "</f**k>"};
}
