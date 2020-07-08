package com.qcm.util.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One master manages many workers
 * Each worker are executed on different thread. Worker object is single in fact.
 * @param <T1>
 * @param <T2>
 */
public class Master<T1, T2> {
    Worker worker;
    int workers;

    /**
     * constructor
     * @param workers number of workers/threads
     * @param seeds products which are consumed by workers
     * @param func the same as consumer, except this method gives out a result
     *             All results are collected by the single worker
     */
    public Master(int workers, LinkedList<T1> seeds, Function<T1, T2> func) {
        this.worker = new Worker<T1, T2>(seeds, func);
        this.workers = workers;
        if (this.workers < 1) {
            this.workers = 1;
        }
    }

    /**
     * constructor
     * @param workers number of workers/threads
     * @param seeds products which are consumed by workers
     * @param consumer consuming method which is executed in one worker. All workers share the same consuming method.
     */
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
