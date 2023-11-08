package com.anven.concurrentDemo;

/**
 * ThreadLocal: 每个线程拥有自己的副本
 */
public class ThreadLocalDemo {
    static ThreadLocal<String> local = new ThreadLocal<>();

    static void print(String str) {
        System.out.println(str + " print and remove var: " + local.get());
        local.remove();
    }

    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            local.set("t1_var");
            print("Thread t1");
            System.out.println("t1 var: " + local.get());
        });

        Thread t2 = new Thread(() -> {
            local.set("t2_var");
            print("Thread t2");
            System.out.println("t1 var: " + local.get());
        });

        t1.start();
        t2.start();
    }
}
