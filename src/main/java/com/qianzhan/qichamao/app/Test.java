package com.qianzhan.qichamao.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    private static Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) {
        logger.error("test error message");
        if (args.length == 0) {
            System.out.println("no args");
        } else {
            System.out.println(String.format("args: %d, 1st is %s", args.length, args[0]));
        }
        logger.info("test info message.");
    }
}
