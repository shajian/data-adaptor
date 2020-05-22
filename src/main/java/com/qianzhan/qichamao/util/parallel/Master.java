package com.qianzhan.qichamao.util.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class Master<T1, T2> {
    Worker worker;
    int workers;
    public Master(int workers, LinkedList<T1> seeds, Function<T1, T2> func) {
        this.worker = new Worker<T1, T2>(seeds, func);
        this.workers = workers;
        if (this.workers < 1) {
            this.workers = 1;
        }
    }

    public Master(int workers, LinkedList<T1> seeds, Consumer<T1> consumer) {
        this.worker = new Worker<T1, T2>(seeds, consumer);
        this.workers = workers;
        if (this.workers < 1) {
            this.workers = 1;
        }
    }
    public List<T2> start() {
        Thread[] threads = new Thread[this.workers];
        for (int i = 0; i < this.workers; ++i) {
            threads[i] = new Thread(worker, String.format("thread %d", i));
            threads[i].start();
        }

        for (int i = 0; i < this.workers; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return worker.getResults();
    }
}
