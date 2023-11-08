package com.anven.concurrentDemo;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public class AtomicTest {
    public static void main(String[] args) {
        int i1 = 1989;
        int i2 = 1989;
        System.out.println(i1 == i2);

        String s1 = "Hello";
        String s2 = "Hello";
        String s3 = new String("Hello"); // 是一个对象，存放在堆内存中
        System.out.println(s1 == s2);
        System.out.println(s1 == s3);
//        AtomicLong
//        Vector
//        LongAdder
//        LongAccumulator
    }
}
