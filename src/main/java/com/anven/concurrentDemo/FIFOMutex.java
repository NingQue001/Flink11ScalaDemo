package com.anven.concurrentDemo;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 先进先出互斥锁
 */
public class FIFOMutex {
    /**
     * 锁标志
     */
    private final AtomicBoolean locked = new AtomicBoolean(false);
    /**
     * 线程等待队列
     */
    private final Queue<Thread> waiters = new ConcurrentLinkedQueue<>();
    /**
     * 获取锁
     */
    public void lock() {

    }

    public void unlock() {
        boolean isInterrupted = false;
        Thread current = Thread.currentThread();
        waiters.add(current);
        while (waiters.peek() != current ||
                !locked.compareAndSet(false, true)) {
            
        }
    }
}
