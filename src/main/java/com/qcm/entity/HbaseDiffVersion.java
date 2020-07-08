package com.qcm.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter@Setter
public class HbaseDiffVersion<T extends IHbaseSerializable<T>> extends HbaseMulti {
    private HbaseMulti<T> pt;
    private HbaseMulti<T> nt;
    private Date date;
}
