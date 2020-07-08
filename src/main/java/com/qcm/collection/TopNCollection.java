package com.qcm.collection;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNCollection<T> {
    private List<T> ts;
    @Getter
    private int n;
    private Comparator<T> comparator;

    public TopNCollection(int n, Comparator<T> comparator) {
        this.n = n;
        this.comparator = comparator;
        this.ts = new ArrayList<>(n);
    }

    public List<T> getUnsafe() {
        return ts;
    }

    public List<T> get() {
        List<T> list = new ArrayList<>(n);
        for (T t : ts) {
            list.add(t);
        }
        return list;
    }

    public void put(T t) {
        int i = 0;
        T old = null;
        for (; i < ts.size(); ++i) {
            if (old != null) {
                t = ts.get(i);
                ts.set(i, old);
                old = t;
            }
            if (comparator.compare(ts.get(i), t) < 0) {
                old = ts.get(i);
                ts.set(i, t);
            }
        }
        if (old == null) {
            ts.add(t);
        } else if (ts.size() < n) {
            ts.add(old);
        }
    }
}
