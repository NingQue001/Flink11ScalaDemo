package com.anven.concurrentDemo;

public class ThreadNotify {
    private static volatile Object resourceA = new Object();
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
           synchronized (resourceA) {
               System.out.println("t1 get resourceA monitor lock");

               try {
                   System.out.println("t1 begin wait");
                   resourceA.wait();
                   System.out.println("t1 end wait");
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
        });

        Thread t2 = new Thread(() -> {
            synchronized (resourceA) {
                System.out.println("t2 get resourceA monitor lock");

                try {
                    System.out.println("t2 begin wait");
                    resourceA.wait();
                    System.out.println("t2 end wait");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t3 = new Thread(() -> {
            synchronized (resourceA) {
                System.out.println("t3 begin notify");
//                resourceA.notify(); // 只会唤醒一个线程
                resourceA.notifyAll();
            }
        });

        t1.start();
        t2.start();
        Thread.sleep(1000);
        t3.start();

        // 等待线程结束
        t1.join();
        t2.join();
        t3.join();

        System.out.println("main thread over");
    }
}
