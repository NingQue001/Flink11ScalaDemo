package com.anven.concurrentDemo;

import java.util.concurrent.atomic.AtomicInteger;

public class A {
    int num = 0;

    AtomicInteger ai = new AtomicInteger();

    public void increase() {
//        synchronized (this) {
//            num ++;
//        }
        ai.incrementAndGet();
    }

    public AtomicInteger getNum() {
//        return num;
        return ai;
    }
}
