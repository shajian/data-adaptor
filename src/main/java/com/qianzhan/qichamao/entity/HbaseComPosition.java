package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;

@Getter@Setter
public class HbaseComPosition implements IHbaseSerializable<HbaseComPosition> {
    private String name;
    private String position;


    @Override
    public HbaseComPosition deepCopy() {
        HbaseComPosition c = new HbaseComPosition();
        c.name = c.name;
        c.position = c.position;
        return c;
    }

    @Override
    public byte[] toBytes(boolean flag) {
        byte length = (byte) (name.length() & 0xff);
        byte[] n = Bytes.toBytes(name);
        byte[] p = Bytes.toBytes(position);
        byte[] bytes = new byte[n.length+p.length+2];
        bytes[0] = length;
        System.arraycopy(n, 0, bytes, 1, n.length);
        bytes[n.length+1] = (byte) (position.length() & 0xff);
        System.arraycopy(p, 0, bytes, n.length+2, p.length);
        return bytes;
    }

    @Override
    public void fromBytes(byte[] bytes) {
        fromBytes(bytes, 0, true);
    }

    @Override
    public int fromBytes(byte[] bytes, int offset, boolean flag) {
        int length = bytes[offset];
        name = Bytes.toString(bytes, offset+1, length>>1);
        int p_length = bytes[offset+length+1];
        position = Bytes.toString(bytes, offset+2+length, p_length>>1);
        return length+p_length+2;
    }
}
