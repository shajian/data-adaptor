package com.qcm.app;

import com.qcm.config.GlobalConfig;
import com.qcm.dal.RedisClient;
import com.qcm.util.Cryptor;
import com.qcm.util.MiscellanyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class Test {
    private static Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) {
        String projectname = System.getProperty("user.dir");
        String pn = projectname.substring(projectname.lastIndexOf('/')+1);
        System.out.println(pn);

//        String template = "f %d, '%s' sdfa";
//        System.out.println(String.format(template, 1, "look", 1.2));
//        GraphParam p = new GraphParam();
//        p.person = "雷军";
//        p.count = 5;
//        p.start = 3;
//        p.pretty = true;
//        CompanyGraph.aggregate(p);

//        logger.error("test error message");
//        if (args.length == 0) {
//            System.out.println("no args");
//        } else {
//            System.out.println(String.format("args: %d, 1st is %s", args.length, args[0]));
//        }
//        logger.info("test info message.");
//        md5();
//        CallTest.sayHello("scala");
//        logger.error("error message");
    }


    public static void test() {
        md5();
    }

    public static void md5() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("md5 generator. please input text:");
        String name = null;
        while (true) {
            name = scanner.nextLine();
            if (name.equals("q")) break;
            if (MiscellanyUtil.isBlank(name)) continue;
            System.out.println("name/md5: "+name+"/"+ Cryptor.md5(name));
        }
        scanner.close();
    }
    public static void t() {
        GlobalConfig.setEnv((byte) 2);
        String name = "深圳前瞻创客科技有限合伙企业（有限合伙）";
        String value = null;
        Scanner scanner = new Scanner(System.in);
        while (true) {
            name = scanner.nextLine();
            if (name.equals("q")) break;
            if (MiscellanyUtil.isBlank(name)) {
                name = RedisClient.randomKey();
                System.out.println("random key: "+name);
            }
            if (name.startsWith("s:")) {
                value = String.join(",", RedisClient.smembers(name));
            } else {
                value = RedisClient.get(name);
            }
            System.out.println("get value: "+value);
        }


        scanner.close();
    }
}
