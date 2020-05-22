package com.qianzhan.qichamao.util.parallel;

public class MasterTest2 {


        public static void main(String[] args) {

            //测试Runnable
            MyThread1 t1 = new MyThread1();
            new Thread(t1).start();//同一个t1，如果在Thread中就不行，会报错
            new Thread(t1).start();
            new Thread(t1).start();

        }



}
