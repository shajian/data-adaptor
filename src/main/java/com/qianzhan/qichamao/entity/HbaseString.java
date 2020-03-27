package com.qianzhan.qichamao.entity;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;

@Getter@Setter
public class HbaseString implements IHbaseSerializable<HbaseString> {
    private String name;

    @Override
    public byte[] toBytes(boolean flag) {
        if (flag) {
            byte[] n = Bytes.toBytes(name);
            byte length = (byte) (n.length & 0xff);
            byte[] bytes = new byte[n.length+1];
            bytes[0] = length;
            System.arraycopy(n, 0, bytes, 1, n.length);
            return bytes;
        } else {
            return Bytes.toBytes(name);
        }
    }

    @Override
    public HbaseString deepCopy() {
        HbaseString str = new HbaseString();
        str.name = this.name;
        return str;
    }

    @Override
    public void fromBytes(byte[] bytes) {
        fromBytes(bytes, 0, false);
    }

    @Override
    public int fromBytes(byte[] bytes, int offset, boolean flag) {
        if (flag) {
            int length = bytes[offset] & 0xff;
            name = Bytes.toString(bytes, offset, length>>1);
            return 1+length;
        } else {
            name = Bytes.toString(bytes, offset);
            return bytes.length - offset;
        }
    }
}
