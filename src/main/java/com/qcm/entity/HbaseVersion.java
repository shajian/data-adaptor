package com.qcm.entity;

import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.ParameterizedType;
import java.util.Date;

@Getter@Setter
public class HbaseVersion<T extends IHbaseSerializable<T>> {
    private HbaseMulti<T> ts;
    private T t;
    private boolean m;
    private Date version;

    private Class<T> getParamClass() {
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        return (Class<T>) type.getActualTypeArguments()[0];
    }

    public byte[] toBytes() {
        if (m) {
            return ts.toBytes();
        } else {
            return t.toBytes(false);
        }
    }

    public void fromBytes(byte[] bytes) throws Exception {
        if (m) {
            if (ts == null) ts = new HbaseMulti<>();
            ts.fromBytes(bytes);
        } else {
            if (t == null)
                t = (T) getParamClass().newInstance();
            t.fromBytes(bytes, 0, false);
        }
    }
}
