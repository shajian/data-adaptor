package com.qcm.app;

import com.qcm.task.maintask.ArangoComTask;
import com.qcm.task.maintask.ESCompanyTask;
import com.qcm.task.maintask.MongoComTask;
import com.qcm.task.maintask.RedisComTask;
import com.qcm.task.stat.BrowseCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Some {
    private static Logger logger = LoggerFactory.getLogger(Some.class);
    public static void main(String[] args) {
        if (args.length == 0 ||
            args.length == 0 && args[0] == "--help") {
            logger.error("no argument is given");
            System.out.println(getHelpInfo());
            return;
        }
        try {
            int taskNo = Integer.parseInt(args[0]);
            if (taskNo == 1) {
                System.out.println("statisticing browsing count...");
                BrowseCount.start();
            } else if (taskNo == 2) {
                System.out.println("writing data into elasticsearch + mongodb...");
                ESCompanyTask writer = new ESCompanyTask();
                writer.start();
            } else if (taskNo == 3) {
                RedisComTask writer = new RedisComTask();
                writer.start();
            } else if (taskNo == 4) {
                ArangoComTask writer = new ArangoComTask();
                ShutdownHook.register(() -> writer.exitSafely());
                writer.start();
            } else if (taskNo == 5) {
                MongoComTask writer = new MongoComTask();
                writer.start();
            } else if (taskNo == 0) {
                Test.test();
            }
            System.out.println("game f**king over");
        } catch (NumberFormatException e) {
            System.out.println(getHelpInfo());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getHelpInfo() {
        String os = System.getProperty("os.name").toLowerCase();
        String format = "start-some.%s [taskno] [--help]\n===============\ntaskno can be:\n\t" +
                "1 - browsing count statistics\n\t" +
                "2 - elasticsearch + mongodb data writing\n\t" +
                "3 - redis company indexing\n" +
                "------------------\n use sh start-some.sh instead of start-some.sh in linux \n\t" +
                "if error 'bash: start-some.sh: Permission denied' pops out.";
        if (os.contains("linux")) {
            return String.format(format, "sh");
        } else if (os.contains("windows")) {
            return String.format(format, ".bat");
        }
        return String.format(format, "sh");
    }
}
