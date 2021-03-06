package com.qcm.es.entity;

import com.qcm.es.EsIndexMeta;
import com.qcm.util.Cryptor;

import java.text.SimpleDateFormat;
import java.util.Date;

@EsIndexMeta(index = "update_log", id = "uid")
public class EsUpdateLogEntity {
    @EsFieldMeta
    public String table_name;
    @EsFieldMeta(doc_values = false)
    public String field_names;
    @EsFieldMeta(doc_values = false)
    public String field_values;
    @EsFieldMeta(type = EsFieldType.date)
    public Date create_time;
    /* time when consume this piece of update meta data */
    @EsFieldMeta(type = EsFieldType.date)
    public Date read_time;
    @EsFieldMeta(type = EsFieldType.bool)
    public boolean status;
    @EsFieldMeta
    public String remark;
    @EsFieldMeta
    public String task_name;

//    @EsFieldMeta(type = EsFieldType.Byte)
//    public byte task_state;

    @EsFieldMeta
    public String uid;

//    // whether is processed normally, or else error thrown out
//    @EsFieldMeta(type = EsFieldType.bool)
//    public boolean error;

    // primary key/id of origin table
    @EsFieldMeta(type = EsFieldType.integer)
    public int tbl_id;

    public void md5() {
        StringBuilder sb = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append(table_name).append(field_names).append(field_values)
                .append(sdf.format(create_time))
                .append(task_name);
        uid = Cryptor.md5(sb.toString());
    }
}
