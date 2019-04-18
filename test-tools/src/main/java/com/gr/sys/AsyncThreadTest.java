package com.gr.sys;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Toolkit for testing multi-threaded and asynchronous applications
 */

public class AsyncThreadTest {

    Queue<Integer> queue = new ConcurrentLinkedQueue<>();


    @Test
    public void shouldDeliverMessage() throws Throwable {
        final Waiter waiter = new Waiter();

        new Thread(()->{
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer message = queue.element();

            waiter.assertEquals(message , 1);

            //notify main thread resume
            waiter.resume();

        }).start();

        //send message
        queue.add(1);
        queue.add(2);
        queue.add(3);


        System.out.println("主线程挂起中，等待子线程通知唤醒");

        //main Thread wait for resume() to be called
        waiter.await(1000);

        System.out.println("子线程通知主线程继续，我才能打印出来哦");
    }
}
