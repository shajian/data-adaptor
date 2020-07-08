package com.qcm.app;


public class ShutdownHook {
    /**
     *
     * @param run can supply a lambda instance
     */
    public static void register(Runnable run) {
        Runtime.getRuntime().addShutdownHook(new Thread(run));
    }
}
