package com.anven.concurrentDemo;

public class InterruptSleep {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
           System.out.println("t1 begin sleep 2000s");
            try {
                Thread.sleep(2000000);
                System.out.println("t1 awake and work"); // 被中断后该打印不会输出
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t1.start();

        Thread.sleep(1000);

        t1.interrupt();
        t1.join();

        System.out.println("main thread over");
    }
}
