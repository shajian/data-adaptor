package com.qcm.entity;

public interface IHbaseSerializable<T> {
    /**
     *
     * @param flag if serialize an Array of T to bytes, true; otherwise false
     * @return
     */
    byte[] toBytes(boolean flag);
    void fromBytes(byte[] bytes);

    /**
     *
     * @param bytes
     * @param offset
     * @param flag if want to get an Array of T from bytes, true; otherwise false
     * @return numbers of bytes consumed from `bytes`
     */
    int fromBytes(byte[] bytes, int offset, boolean flag);

    T deepCopy();
}
