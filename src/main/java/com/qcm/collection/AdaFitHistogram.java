package com.qcm.collection;

import com.qcm.util.MiscellanyUtil;

/**
 * Statistics distribution is biased severely sometimes. Based on it, scoring are often disappointing.
 * We need to make a histogram and let it fit some kind of good discrete distribution, such as
 * uniform distribution, gaussian distribution, etc.
 *
 * But, how to do? Please check this file.
 *
 * NOTICE: Because of the big data(massive amount), we have no choice but to iterate batch by batch
 * instead of loading all data just once, and we can only promise the approximate fitness to some distribution.
 */
public class AdaFitHistogram {
    /**
     * (0, 2],
     * (2, 4],
     */
    private int[] buckets;
    private int span;

    /**
     * score will be:
     * 0, 1, 2, ..., n
     */
    private int n;
    /**
     *  (0, b1]         ======>>>>> 0 is excluded <<<<<======  score: 1
     *  (b1, b2]                                               score: 2
     *  ...
     *  (bn-2, bn-1]
     *  (bn-1, inf]                                            score: n
     *  value:  b1, b2, b3, ... , bn-2, bn-1
     *  index:  0 ,  1,  2, ... , n-3 , n-2
     *  score: 1,  2,  3, ...,  n-2,  n-1 ,  n
     */
    private int[] boundaries;

    /**
     * 0: uniform
     * 1. gaussian
     */
    private byte type;

    public AdaFitHistogram() {}

    public AdaFitHistogram(int num) throws Exception {
        this(num, 2500, 1, (byte) 0);
    }

    public AdaFitHistogram(int num, int bucket_num, int span, byte t) throws Exception {
        buckets = new int[bucket_num];
        boundaries = new int[num-1];
        n = num;
        this.span = span;
        type = t;
        if (type == 1) {
            if (n % 2 != 1) {
                throw new Exception("number of ranges should be odd for gaussian distribution.");
            }
        }
    }

    public void adapt(int[] counts) {
        for (int i = 0; i < counts.length; ++i) {
            if (counts[i] <= 0) continue;
            int index = (counts[i] + span - 1) / span - 1;
            if (index >= buckets.length) {
                index = buckets.length - 1;
            }
            buckets[index] += 1;
        }
    }

    public void gen() {
        int sum = 0;
        for (int i = 0; i < buckets.length; ++i) sum += buckets[i];
        int thre = sum/n;       // for type 'uniform'

        int pre = 0;
        int post = 0;
        int j = 0;
        for (int i = 0; i < buckets.length; ++i) {
            post += buckets[i];
            // int thre = getGaussian(j);   // for type 'gaussian'
            if (post >= thre && pre < thre) {
                if (j < n-1) {
                    boundaries[j] = (i+1)*span;
                }
                j++;
                post = pre = 0;
            } else {
                pre = post;
            }
        }

        buckets = null;
    }

    public int getScore(int count) {
        if (count <= 0) return 0;
        for (int i = 0; i < n-2; ++i) {
            if (count <= boundaries[i]) return i+1;
        }
        return n;
    }

//    private int getScore(int count, int i, int j) {
//        int index = (i+j)/2;
//        if (count <= boundaries[index]) {
//            if (index == 0 || count > boundaries[index-1]) return index+1;
//            return getScore(count, i, index-1);
//        }
//        // count > boundaries[index]
//        if (index == n-2 || count <= boundaries[index+1]) return index+2;
//        return getScore(count, index+1, j);
//    }

    public byte[] serialize() {
        byte[] bytes = new byte[boundaries.length*4+4];
        System.arraycopy(MiscellanyUtil.int2bytes(n), 0, bytes, 0, 4);
        for (int i = 0; i < boundaries.length; ++i) {
            System.arraycopy(MiscellanyUtil.int2bytes(boundaries[i]), 0, bytes, (i+1)*4, 4);
        }
        return bytes;
    }

    /**
     *
     * @param bytes
     * @param start
     * @return number of bytes consumed by this method
     */
    public int deSerialize(byte[] bytes, int start) {
        this.n = MiscellanyUtil.bytes2int(bytes, start);
        this.boundaries = new int[this.n];
        for (int i = 0; i < this.n; ++i) {
            this.boundaries[i] = MiscellanyUtil.bytes2int(bytes, start+i*4+4);
        }
        return 4*(n+1);
    }
}
