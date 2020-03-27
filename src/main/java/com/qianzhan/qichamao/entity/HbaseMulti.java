package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

@Getter@Setter
public class HbaseMulti<T extends IHbaseSerializable<T>> {
    private T[] ts;
    private T seed;


    public void setSeed(T t) {
        seed = t;
    }

    private Class<T> getParamClass() {
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) type.getActualTypeArguments()[0];
    }

    public byte[] toBytes() {
        int size = 0;
        byte[][] bytesList = new byte[ts.length][];
        for (int i = 0; i < ts.length; ++i) {
            bytesList[i] = ts[i].toBytes(true);
            size += bytesList[i].length;
        }
        byte[] bytes = new byte[size];
        int offset = 0;
        for (int i = 0; i < ts.length; ++i) {
            System.arraycopy(bytesList[i], 0, bytes, offset, bytesList[i].length);
            offset += bytesList[i].length;
        }
        return bytes;
    }

    public void fromBytes(byte[] bytes) throws Exception {
        List<T> list = new ArrayList<>();
        int offset = 0;
        int length = 0;
        while (offset < bytes.length) {
            if (seed != null) {
                length = seed.fromBytes(bytes, offset, true);
                list.add(seed.deepCopy());
            } else {
                T t = (T) getParamClass().newInstance();
                length = t.fromBytes(bytes, 0, false);
            }
            offset += length;
        }
        ts = (T[]) list.toArray();
    }
}
