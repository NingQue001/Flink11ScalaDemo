package com.anven.concurrentDemo;

/**
 * Double Check Lock
 */
public class DoubleCheckLockSingleton {
    private static DoubleCheckLockSingleton instance = null;

    private DoubleCheckLockSingleton() {

    }

    // DCL
    public static DoubleCheckLockSingleton getInstance() {
        if (instance == null) {
            synchronized (DoubleCheckLockSingleton.class) {
                if (instance == null) {
                    instance = new DoubleCheckLockSingleton();
                }
            }
        }

        return instance;
    }

    public static void main(String[] args) {
        DoubleCheckLockSingleton dcl = DoubleCheckLockSingleton.getInstance();
    }
}
