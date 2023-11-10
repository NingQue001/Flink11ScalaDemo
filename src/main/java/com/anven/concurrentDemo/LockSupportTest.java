package com.anven.concurrentDemo;

import java.util.concurrent.locks.LockSupport;

public class LockSupportTest {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            System.out.println("child thread begin park");

            // 调用 park方法，挂起自己
            LockSupport.park();

            System.out.println("child thread unpark");
        });

        t1.start();

        Thread.sleep(1000);

        System.out.println("mian thread begin unpark");
        LockSupport.unpark(t1);

        System.out.println("mian thread over");
    }
}
