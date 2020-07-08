package com.qcm.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.List;

@Getter
@Setter
@ToString
public class TestCase {
    private int id;
    private Date date;
    private String name;
    private Long uid;
    private byte status;
    private boolean flag;
    private Boolean init;
    private List<String> strs;
    private int[] ints;
}
