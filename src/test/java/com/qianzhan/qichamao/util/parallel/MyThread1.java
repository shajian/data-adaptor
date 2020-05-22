package com.qianzhan.qichamao.util.parallel;

class MyThread1 implements Runnable{
    private int ticket = 10;
    @Override
    //记得要资源公共，要在run方法之前加上synchronized关键字，要不然会出现抢资源的情况
    public synchronized  void  run() {
        for (int i = 0; i <10; i++) {
            if (this.ticket>0) {
                System.out.println("卖票：ticket"+this.ticket--);
            }
        }

    }

}