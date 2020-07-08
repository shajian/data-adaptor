package com.qcm.entity;

import com.qcm.util.MiscellanyUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

@Getter@Setter
public class HbaseComInvest implements IHbaseSerializable<HbaseComInvest> {
    private String name;    // name of investor or investee
    private float value;    // money (unit: 10 thousand RMB

    /**
     * for invest-in: ignore this field when serializing to save memory
     */
    private float ratio;    //
    /**
     * invest in or out?
     */
    private boolean in;

    public HbaseComInvest() {}
    public HbaseComInvest(String name, float value, float ratio, boolean in) {
        if (MiscellanyUtil.isBlank(name)) {
            this.name = "";
        } else if (name.length() > 63) {
            this.name = name.substring(0, 63);
        } else {
            this.name = name;
        }
        this.value = value;
        this.ratio = ratio;
        this.in = in;
    }

    @Override
    public byte[] toBytes(boolean flag) {
        byte[] v = Bytes.toBytes(value);
        byte[] n = Bytes.toBytes(name);
        int d = flag ? 1 : 0;
        int r_len = in ? 0 : 4;
        byte[] bs = new byte[v.length+r_len+n.length+d];
        System.arraycopy(v, 0, bs, 0, v.length);    // serialize date
        if (!in) {
            System.arraycopy(Bytes.toBytes(ratio), 0, bs, v.length, 4);
        }

        if (flag) {
            bs[v.length+r_len] = (byte) (n.length & 0xff);
        }
        System.arraycopy(n, 0, bs, v.length+r_len+d, n.length);
        return bs;
    }

    @Override
    public void fromBytes(byte[] bytes) {
        fromBytes(bytes, 0, false);
    }

    @Override
    public HbaseComInvest deepCopy() {
        HbaseComInvest invest = new HbaseComInvest();
        invest.name = this.name;
        invest.value = this.value;
        invest.ratio = this.ratio;
        invest.in = this.in;
        return invest;
    }

    /**
     *
     * @param bytes
     * @param offset
     * @param flag
     * @return
     */
    @Override
    public int fromBytes(byte[] bytes, int offset, boolean flag) {
        value = Bytes.toFloat(bytes, offset);
        int r_len = 0;
        if (!in) {
            ratio = Bytes.toFloat(bytes, 4 + offset);
            r_len = 4;
        }
        if (flag) {
            int length = (bytes[offset+4+r_len] & 0xff) >> 1;
            name = Bytes.toString(bytes, offset+4+1+r_len, length);
            return 4+r_len+1+length*2;
        } else {
            name = Bytes.toString(bytes, 4+r_len+offset);
            return bytes.length-offset;
        }
    }

    /**
     * sort ascend
     */
    public final static Comparator<HbaseComInvest> comparator = new Comparator<HbaseComInvest>() {
        @Override
        public int compare(HbaseComInvest o1, HbaseComInvest o2) {
            if (o1.value < o2.value) return 1;
            if (o1.value > o2.value) return -1;
            return 0;
        }
    };
}
