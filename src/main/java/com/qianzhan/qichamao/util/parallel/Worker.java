package com.qianzhan.qichamao.util.parallel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;

public class Worker<T1, T2> implements Runnable {
    private ConcurrentLinkedQueue<T1> seeds;
    private ConcurrentLinkedQueue<T2> results;
    private Function<T1, T2> func;
    private Consumer<T1> consumer;
    public Worker(List<T1> seeds, Function<T1, T2> func) {
        this.seeds = new ConcurrentLinkedQueue<>();
        this.seeds.addAll(seeds);
        this.func = func;
        this.results = new ConcurrentLinkedQueue<>();
    }
    public Worker(List<T1> seeds, Consumer<T1> consumer) {
        this.seeds = new ConcurrentLinkedQueue<>();
        this.seeds.addAll(seeds);
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (seeds.size()>0) {
            T1 seed = seeds.poll();
            if (consumer != null) {
//                System.out.println(Thread.currentThread().getName()+" print " + seed);
                consumer.accept(seed);
            } else {
                results.add(func.apply(seed));
            }
        }
    }

    public List<T2> getResults() {
        if (results == null) return null;

        List<T2> list = new ArrayList<>();
        while (results.size() > 0) {
            list.add(results.poll());
        }
        return list;
    }
}
