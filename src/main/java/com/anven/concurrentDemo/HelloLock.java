package com.anven.concurrentDemo;

/**
 * synchronized ：独占锁，可重入锁
 */
public class HelloLock {
    public synchronized void helloA() {
        System.out.println("Hello A");
    }

    public synchronized void helloB() {
        System.out.println("Hello B");
        helloA();
    }

    public static void main(String[] args) {
        HelloLock hl = new HelloLock();
        hl.helloB();
    }
}
