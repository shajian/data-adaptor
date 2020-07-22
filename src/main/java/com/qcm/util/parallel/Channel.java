package com.qcm.util.parallel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * A completed pipeline consists of many channels, where each channel represents a processing phrase.
 * Data flows through pipeline, and be mapped once after a channel.
 */
public class Channel<Tin, Tout> {
    private boolean sync;   //
    private BlockingQueue<Tin> container;
    private Function<Tin, Tout> func;
    public Channel(BlockingQueue<Tin> container) {
        this.container = container;
    }
    public void setFunc(Function<Tin, Tout> func) {
        this.func = func;
    }
    public void receive(Tin t) throws InterruptedException {
        container.put(t);     // block when full
    }

    private void work() throws InterruptedException {
        Tin t;
        while (true) {
            t = container.take(); // block when empty
            try {
                Tout out = func.apply(t);
            } catch (Exception e) {
                // todo log this error
            }
        }
    }
}
