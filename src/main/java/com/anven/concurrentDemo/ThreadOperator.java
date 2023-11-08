package com.anven.concurrentDemo;

/**
 * Thread方法：
 * wait()
 * join(): 等待线程执行终止的方法
 * Interrupter()
 *
 *
 */
public class ThreadOperator {
//    public static void main(String[] args) throws InterruptedException {
//        Thread t1 = new Thread(() -> {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("Thread 1 over");
//        });
//
//        Thread t2 = new Thread(() -> {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("Thread 2 over");
//        });
//
//        t1.start();
//        t2.start();
//
//        System.out.println("wait all Thread over...");
//
//        t1.join();
//        t2.join();
//
//        System.out.println("all Thread over！");
//    }

    public static void main(String[] args) throws InterruptedException {
        //线程one
        Thread threadOne = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("threadOne begin run! ");
                for (; ; ) {
                    System.out.println("旋转跳跃...");
                }
            }
        });
        //获取主线程
        final Thread mainThread = Thread.currentThread();
        //线程two
        Thread threadTwo = new Thread(new Runnable() {
            @Override
            public void run() {
                //休眠1s
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //中断主线程
                mainThread.interrupt();
//                threadOne.interrupt(); // 不起作用，无法中断线程1
            }
        });
        // 启动子线程
        threadOne.start();
        //延迟1s启动线程
        threadTwo.start();
        try{//等待线程one执行结束
            threadOne.join(); // 线程1仍然会继续运行下去
        }catch(InterruptedException e){
            System.out.println("main thread:" + e);
        }
    }
}
