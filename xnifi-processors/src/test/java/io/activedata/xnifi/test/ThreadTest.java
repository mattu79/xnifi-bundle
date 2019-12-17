package io.activedata.xnifi.test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadTest {
    static class Task1 implements Runnable {
        @Override
        public void run() {
            int total = 10;
            for (int i = 0; i <= total; i++) {
                try {
                    Thread.sleep(50);
                    System.err.println(this.toString() + " sleep 50ms:" + i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void main(String[] args) {
//        Runnable task = () -> {
//
//
//        };

        ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2);

        for (int j = 0; j <= 10; j++) {
            pool.scheduleWithFixedDelay(
                    new Task1(), 0, 4, TimeUnit.SECONDS);
            System.err.println(pool.getPoolSize() + "|" + pool.getActiveCount() + "|" + pool.getTaskCount());
        }
    }
}
