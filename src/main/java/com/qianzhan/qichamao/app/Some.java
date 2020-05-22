package com.qianzhan.qichamao.app;

import com.qianzhan.qichamao.task.com.ArangodbCompanyWriter;
import com.qianzhan.qichamao.task.com.EsCompanyWriter;
import com.qianzhan.qichamao.task.com.MongodbCompanyWriter;
import com.qianzhan.qichamao.task.com.RedisCompanyIndexWriter;
import com.qianzhan.qichamao.task.stat.BrowseCount;

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
                EsCompanyWriter writer = new EsCompanyWriter();
                writer.start();
            } else if (taskNo == 3) {
                RedisCompanyIndexWriter writer = new RedisCompanyIndexWriter();
                writer.start();
            } else if (taskNo == 4) {
                ArangodbCompanyWriter writer = new ArangodbCompanyWriter();
                ShutdownHook.register(() -> writer.exitSafely());
                writer.start();
            } else if (taskNo == 5) {
                MongodbCompanyWriter writer = new MongodbCompanyWriter();
                writer.start();
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
