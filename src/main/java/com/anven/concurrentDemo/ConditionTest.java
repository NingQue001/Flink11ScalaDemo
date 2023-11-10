package com.anven.concurrentDemo;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AQS: 条件变量
 */
public class ConditionTest {
    public static void main(String[] args) throws InterruptedException {
        ReentrantLock lock = new ReentrantLock();
        Condition condition1 = lock.newCondition();
        Condition condition2 = lock.newCondition();

        Thread t1 = new Thread(() -> {
            lock.lock();
            try {
                System.out.println("吕布：等貂蝉过来再议");
                condition1.await();
                System.out.println("吕布：貂蝉说的在理");
                condition2.signal();
                System.out.println("董卓发话");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        });

        Thread t2 = new Thread(() -> {
            lock.lock();
            try {
                System.out.println("貂蝉来了");
                condition1.signal();
                System.out.println("貂蝉说: 我是一女子，不便议政");
                condition2.await();
                System.out.println("董卓：那就这么决定...");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        });


        t1.start();
        t2.start();

        Thread.sleep(1000);
        t1.join();
        t2.join();

        System.out.println("结束对话");
    }
}
