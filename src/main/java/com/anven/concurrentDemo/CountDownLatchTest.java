package com.anven.concurrentDemo;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchTest {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch cdl = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("我是小明");
            cdl.countDown();
        });

        Thread t2 = new Thread(() -> {

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("我是大胖");
            cdl.countDown();
        });

        t1.start();
        t2.start();

        cdl.await();
        System.out.println("人齐，开始干活");
    }
}
