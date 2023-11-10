package com.anven.concurrentDemo;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 使用 ReentrantReadWriteLock 实现 线程安全并且读写分离的 List
 */
public class ReentrantReadWriteLockList {
    // 线程不安全的 list
    private ArrayList<String> array = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public void add(String s) {
        writeLock.lock();
        try {
            array.add(s);
        } finally {
            writeLock.unlock();
        }
    }

    public void remove(String s) {
        writeLock.lock();
        try {
            array.remove(s);
        } finally {
            writeLock.unlock();
        }
    }

    public String get(int idx) {
        readLock.lock();
        try {
            return array.get(idx);
        } finally {
            readLock.unlock();
        }
    }
}
