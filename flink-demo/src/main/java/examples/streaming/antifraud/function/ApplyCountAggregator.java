package examples.streaming.antifraud.function;

import examples.streaming.antifraud.model.ApplyMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyCountAggregator implements AggregateFunction<ApplyMessage, ApplyCountAggregator.MyCountAccumulator, Tuple2<String,Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplyCountAggregator.class);

    @Override
    public MyCountAccumulator createAccumulator() {
        return new MyCountAccumulator();
    }

    @Override
    public MyCountAccumulator add(ApplyMessage applyMessage, MyCountAccumulator myCountAccumulator) {

        LOGGER.info("事件总数 {}, IdCard {}, last IdCard {}",myCountAccumulator.count + 1, applyMessage.getIdCard(), myCountAccumulator.idCard);
        myCountAccumulator.idCard = applyMessage.getIdCard();
        myCountAccumulator.count += 1;
        return myCountAccumulator;
    }

    @Override
    public Tuple2<String, Integer> getResult(MyCountAccumulator myCountAccumulator) {

        LOGGER.info("函数执行得到结果 {}",myCountAccumulator.count);

        return Tuple2.of(myCountAccumulator.idCard, myCountAccumulator.count);
    }

    @Override
    public MyCountAccumulator merge(MyCountAccumulator myCountAccumulator, MyCountAccumulator acc1) {
        myCountAccumulator.count += acc1.count;
        return myCountAccumulator;
    }

    static class MyCountAccumulator{
        String idCard;
        Integer count = 0;
    }
}


