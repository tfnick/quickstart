package com.gr.sys;

import com.lzh.easythread.AsyncCallback;
import com.lzh.easythread.EasyThread;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ThreadPool {

    private static Logger logger = LoggerFactory.getLogger(ThreadPool.class);


    /**
     * Runnable 普通任务（不带返回值）
     * Callable 待返回值任务
     * AsyncCallback 异步回调任务
     *
     * 延迟执行任务
     */

    public static void main(String[] args) throws Exception{
        EasyThread easyThread = EasyThread.Builder.createSingle().build();

        easyThread.execute(new Runnable(){
            @Override
            public void run() {
                // do something.
            }
        });


        Future<Integer> task = easyThread.submit(new Callable<Integer>(){

            @Override
            public Integer call() throws Exception {
                return 64 * 64;
            }
        });
        Integer result = task.get();


        easyThread.setDelay(3, TimeUnit.SECONDS)
                .execute(()->{
                    System.out.println("task is executed");
                });



        Callable<Integer> callable = ()->{
            System.out.println("task is executed");
            return 1024;
        };



        AsyncCallback<Integer> async = new AsyncCallback<Integer>() {
            @Override
            public void onSuccess(Integer o) {

            }

            @Override
            public void onFailed(Throwable throwable) {

            }
        };

        easyThread.async(callable, async);

    }

    /**
     * 单个线程超时监控
     */
    public void singlTaskTimeout(){
        EasyThread easyThread = EasyThread.Builder.createSingle().build();

        Future<Integer> task = easyThread.submit(new Callable<Integer>(){

            @Override
            public Integer call() throws Exception {
                return 64 * 64;
            }
        });
        Integer result = null;

        try {
            task.get(1000,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            logger.error("当前任务执行超时，强行结束任务");
            task.cancel(true);
        }finally {
            easyThread.getExecutor().shutdown();
        }
    }

    /**
     * 线程池超时监控,超过3秒线程池中任务还没有执行完，则取消还没有执行完的线程
     */
    public void multiTaskTimeout() throws InterruptedException{
        EasyThread easyThread = EasyThread.Builder.createSingle().build();

        Callable<Integer> task1 = () -> {return 1;};
        Callable<Integer> task2 = () -> {return 2;};

        Future<Integer> f1 = easyThread.submit(task1);
        Future<Integer> f2 = easyThread.submit(task2);

        if(easyThread.getExecutor().awaitTermination(3, TimeUnit.SECONDS)){
            System.out.println("all task finished");
        }else {
            Lists.newArrayList(f1,f2).forEach(f -> {
                if(!f.isDone()){
                    f.cancel(true);
                }
            });
        }
    }
}
