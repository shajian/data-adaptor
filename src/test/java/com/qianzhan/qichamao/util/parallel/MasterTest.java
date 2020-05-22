package com.qianzhan.qichamao.util.parallel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class MasterTest {
    public static void main(String[] args) {
//        consumeTest();
        List<String> codes = new ArrayList<>();
        for (int i = 0; i < 40; ++i) {
            if (i == 20) codes.add(null);
            codes.add("fuck");
        }
        System.out.println(String.format("%s fucking", "m"));
    }

    private static void consumeTest() {
        List<Integer> seeds = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            seeds.add(i);
        }
        LinkedList<Integer> queue = new LinkedList<>();
        queue.addAll(seeds);
        Consumer<Integer> consumer = i -> System.out.println(i);
        Master<Integer, Void> master = new Master<Integer, Void>(3, queue, consumer);
        master.start();
    }

    private static void funtionTest() {
        List<Integer> seeds = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            seeds.add(i);
        }
        LinkedList<Integer> queue = new LinkedList<>();
        queue.addAll(seeds);
        Function<Integer, String> func = i -> i.toString();
        Master<Integer, String> master = new Master<Integer, String>(3, queue, func);
        List<String> results = master.start();

        for (String result : results) {
            System.out.println(result);
        }
    }
}
