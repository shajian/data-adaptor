package com.qianzhan.qichamao.dal.mybatis;

public interface CommonMapper {
    Integer getCheckpoint(String key);
    void insertCheckpoint0(String key);
    void insertCheckpoint(String key, int value);
    void updateCheckpoint(String key, int value);
}
