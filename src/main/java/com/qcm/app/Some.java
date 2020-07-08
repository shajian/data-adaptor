package com.qcm.app;

import com.qcm.task.com.MainTaskArangodbCompany;
import com.qcm.task.com.MainTaskEsCompany;
import com.qcm.task.com.MainTaskMongodbCompany;
import com.qcm.task.com.MainTaskRedisCompanyIndex;
import com.qcm.task.stat.BrowseCount;

public class Some {
    public static void main(String[] args) {
        if (args.length == 0 ||
            args.length == 0 && args[0] == "--help") {
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
                MainTaskEsCompany writer = new MainTaskEsCompany();
                writer.start();
            } else if (taskNo == 3) {
                MainTaskRedisCompanyIndex writer = new MainTaskRedisCompanyIndex();
                writer.start();
            } else if (taskNo == 4) {
                MainTaskArangodbCompany writer = new MainTaskArangodbCompany();
                ShutdownHook.register(() -> writer.exitSafely());
                writer.start();
            } else if (taskNo == 5) {
                MainTaskMongodbCompany writer = new MainTaskMongodbCompany();
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
