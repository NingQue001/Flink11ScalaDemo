package com.anven.concurrentDemo;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CyclicBarrierTest {
    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {
        // 初始化需要传入控制的线程数
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

        Thread t1 = new Thread(() -> {
            try {
                System.out.println("美国打贸易战");
                // 通过调用CyclicBarrier的await()方法进入等待状态，通常在线程完成自己的阶段性任务之后调用该方法。
                cyclicBarrier.await();
                System.out.println("美国打金融战");
                cyclicBarrier.await();
                System.out.println("美帝扛不住了");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(500);
                System.out.println("欧洲小弟也跟着打贸易战");
                cyclicBarrier.await();
                Thread.sleep(500);
                System.out.println("欧洲小弟也跟着打金融战");
                cyclicBarrier.await();
                Thread.sleep(500);
                System.out.println("欧洲小弟也扛不住了");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();

    }
}
