package com.qianzhan.qichamao.config;

public class GlobalConfig {
    // environment: it is common that many differences exist between TEST and PUBLISH
    private static byte env;
    public static byte getEnv() {
        return env;
    }
    public static void setEnv(byte env_) {
        env = env_;
    }
}
