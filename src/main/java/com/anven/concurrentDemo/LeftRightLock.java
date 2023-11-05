package com.anven.concurrentDemo;

/**
 * 顺序锁
 *
 * https://zhuanlan.zhihu.com/p/385855265
 */
public class LeftRightLock {
    private final Object right = new Object();
    private final Object left = new Object();

    /**
     * 加锁顺序从left -> right
     */
    public void leftToRight() {
        synchronized (left) {
            synchronized (right) {
                System.out.println(Thread.currentThread().getName() + " left -> right lock.");
            }
        }
    }

    /**
     * 加锁顺序right -> left
     */
    public void rightToLeft() {
        synchronized (right) {
            synchronized (left) {
                System.out.println(Thread.currentThread().getName() + " right -> left lock.");
            }
        }
    }

    public static void main(String[] args) {
        LeftRightLock lrDeadLock = new LeftRightLock();

        // 在多线程环境中才会发生死锁，如果单线程不会出现问题
//        leftRightLock.left2Right();
//        leftRightLock.right2Left();
//        System.out.println("执行完成");


        // 多线程环境会发生死锁（10个线程还不一定会产生死锁，加大线程数一定可以复现死锁场景）
        for (int i = 0; i < 1000; i++) {
            new Thread(() -> {
                // 为了更好的演示死锁，将两个方法的调用放置到同一个线程中执行
                lrDeadLock.leftToRight();
                lrDeadLock.rightToLeft();
            }, "ThreadA-" + i).start();
        }
    }
}
