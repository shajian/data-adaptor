package com.qcm.dal.mybatis;

import org.apache.ibatis.annotations.Param;

public interface CommonMapper {
    Integer getCheckpoint(String key);
    void insertCheckpoint0(String key);
    void insertCheckpoint(@Param("key") String key, @Param("value") int value);
    void updateCheckpoint(@Param("key") String key, @Param("value") int value);
}
