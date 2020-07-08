package com.qcm.config;

public class GlobalConfig {
    // environment: it is common that many differences exist between TEST and PUBLISH
    private static byte env = 1;
    public static byte getEnv() {
        return env;
    }
    public static void setEnv(byte env_) {
        env = env_;
    }
}
