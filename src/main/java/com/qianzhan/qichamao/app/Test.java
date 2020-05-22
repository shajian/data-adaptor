package com.qianzhan.qichamao.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    private static Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) {
        String template = "f %d, '%s' sdfa";
        System.out.println(String.format(template, 1, "look", 1.2));



//        logger.error("test error message");
//        if (args.length == 0) {
//            System.out.println("no args");
//        } else {
//            System.out.println(String.format("args: %d, 1st is %s", args.length, args[0]));
//        }
//        logger.info("test info message.");
    }
}
