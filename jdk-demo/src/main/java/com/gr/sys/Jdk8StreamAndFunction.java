package com.gr.sys;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Character.isDigit;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;


/**
 * http://www.cnblogs.com/snowInPluto/p/5981400.html
 */
public class Jdk8StreamAndFunction {


    /**
     * collect(toList()) 方法由 Stream 里的值生成一个列表，是一个及早求值操作。可以理解为 Stream 向 Collection 的转换。
     */
    @Test
    public void testCollect(){
        List<String> collected = Stream.of("a", "b", "c").collect(toList());
        assertEquals(asList("a", "b", "c"), collected);
    }

    /**
     * map方法就是接受一个 Function 的匿名函数，将一个流中的值转换成一个新的流。
     */
    @Test
    public void testMap(){
        List<String> collected = Stream.of("a", "b", "hello")
                .map(string -> string.toUpperCase())
                .collect(toList());
        assertEquals(asList("A", "B", "HELLO"), collected);
    }

    /**
     * filter 方法就是接受的一个 Predicate 的匿名函数类，判断对象是否符合条件，符合条件的才保留下来。
     */
    @Test
    public void testFilter(){
        List<String> beginningWithNumbers =
                Stream.of("a", "1abc", "abc1")
                        .filter(value -> isDigit(value.charAt(0)))
                        .collect(toList());
        assertEquals(asList("1abc"), beginningWithNumbers);
    }


    /**
     * 所谓flatMap，就是对父Stream A中每个元素先转换为子Stream B，然后内部自动把各个子Stream B中的所有元素合并成一个新的Stream C
     */
    /**
     * Stream of List
     */
    @Test
    public void testFlatMap1(){
        List<Integer> together = Stream.of(asList(1, 2), asList(3, 4))
                .flatMap(numbers -> numbers.stream())
                .collect(toList());
        assertEquals(asList(1, 2, 3, 4), together);
    }

    /**
     * List to Stream
     */
    @Test
    public void testFlatMap2(){
        List<String> list = Arrays.asList("beijing changcheng", "beijing gugong", "beijing tiantan","gugong tiananmen");
        list.stream().flatMap(item -> Arrays.stream(item.split(" ")))
                     .collect(Collectors.toList())
                     .forEach(System.out::println);
    }

    @Test
    public void testMaxMin(){
        List<Integer> list = Lists.newArrayList(3, 5, 2, 9, 1);
        int maxInt = list.stream()
                .max(Integer::compareTo)
                .get();
        int minInt = list.stream()
                .min(Integer::compareTo)
                .get();
        assertEquals(maxInt, 9);
        assertEquals(minInt, 1);
    }


    /**
     * 累加
     * reduce 操作可以实现从一组值中生成一个值。在上述例子中用到的 count、min 和 max 方法,因为常用而被纳入标准库中。事实上，这些方法都是 reduce 操作。
     */
    @Test
    public void testReduce1(){
        int result = Stream.of(1, 2, 3, 4)
                .reduce(0, (acc, element) -> acc + element);
        assertEquals(10, result);
    }

    /**
     * 累乘
     * reduce 操作可以实现从一组值中生成一个值。在上述例子中用到的 count、min 和 max 方法,因为常用而被纳入标准库中。事实上，这些方法都是 reduce 操作。
     */
    @Test
    public void testReduce2(){
        int result = Stream.of(1, 2, 3, 4)
                .reduce(1, (acc, element) -> acc * element);
        assertEquals(24, result);
    }

    /**
     * fork join模式，自动开辟线程去做，这里面需要区分场景是否适合
     */
    @Test
    public void testParallel(){
        int sumSize = Stream.of("Apple", "Banana", "Orange", "Pear")
                .parallel()
                .map(s -> s.length())
                .reduce(Integer::sum)
                .get();
        assertEquals(sumSize, 21);
    }
}
