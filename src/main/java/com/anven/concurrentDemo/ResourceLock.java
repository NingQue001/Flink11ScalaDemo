package com.anven.concurrentDemo;

public class ResourceLock {
    private static volatile Object resourceA = new Object();
    private static volatile Object resourceB = new Object();
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
           synchronized (resourceA) {
               System.out.println("t1 get resourceA monitor lock");

               synchronized (resourceB) {
                   System.out.println("t1 get resourceB monitor lock");

                   System.out.println("t1 release resourceA monitor lock");
                   try {
                       resourceA.wait();
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
               }
           }
        });

        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(1000);

                synchronized (resourceA) {
                    System.out.println("t2 get resourceA monitor lock");

                    System.out.println("t2 try get resourceB monitor lock");
                    synchronized (resourceB) {
                        System.out.println("t2 get resourceB monitor lock");

                        System.out.println("t2 release resourceA monitor lock");
                        resourceA.wait();

                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.println("main thread over");
    }
}
