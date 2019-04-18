package com.gr.sys;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

/**
 * https://www.cnblogs.com/figure9/p/java-8-lambdas-insideout-language-features.html
 * 1，lamda表达式的类型是推导出来的，同样的表达式在不同的环境中对应的目标类型（Target typing）是不同的
 * 2，表达式的目标类型确定了，从而编译器可以反向推导出表达式形参的类型
 */
public class Jdk8Lamda {


    public static void main(String[] args) {

    }

    @Test
    public void lamdaForEach() {
        List<Integer> list = Lists.newArrayList(2, 3, 1);

        list.forEach(e -> {
            if (e > 1) {
                System.out.println(e);
            }
        });
    }

    @Test
    public void lamdaFilter() {
        //List<Integer> list = Lists.newArrayList(2, 3, 1);

        List<Integer> collected = Stream.of(2, 3, 1).filter(num -> num >= 2).collect(toList());
        assertEquals(asList(2,3), collected);
    }


    /**
     * 文件过滤的简化例子
     */
    public void lamda1() {
        //1.文件目录
        File fileDir = new File("D:/resource");
        //2.筛选
        File[] files = fileDir.listFiles((f) -> !f.isDirectory() && f.getName().endsWith(".js"));
    }

    public void lamda1Old() {
        //1.文件目录
        File fileDir = new File("D:/resource");

        FileFilter fileFilter = new FileFilter() {
            @Override
            public boolean accept(File f) {
                return f.isDirectory() && f.getName().endsWith(".js");
            }
        };
        //2.筛选
        File[] files = fileDir.listFiles(fileFilter);
    }

    /**
     * 子lamda表达式【() -> {System.out.println("hi");};】作为父lamda表达式的表达式部分
     */
    public void lamda2() {
        Supplier<Runnable> c = () -> () -> {
            System.out.println("hi");
        };
    }

    public void lamda2Old() {
        Supplier<Runnable> c = new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("hi");
                    }
                };
            }
        };
    }

    /**
     * 函数式编程与条件表达式
     */
    public void lamda3() {
        boolean flag = true;
        Callable<Integer> call = flag ? (() -> 23) : (() -> 42);
    }

    public void lamda3Old() {
        boolean flag = true;
        Callable<Integer> a = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 23;
            }
        };

        Callable<Integer> b = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 42;
            }
        };

        Callable<Integer> call = flag ?  a : b;
    }
}
