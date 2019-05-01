package examples.streaming.antifraud.function;

import examples.streaming.antifraud.model.ApplyMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowCountApplyFunction implements WindowFunction<ApplyMessage, Tuple3<String, Double, String>, Tuple, TimeWindow> {



    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ApplyMessage> iterable, Collector<org.apache.flink.api.java.tuple.Tuple3<String, Double, String>> collector) throws Exception {
        Double sum = 0d;
        String key = "money";
        String gid = null;

        for (ApplyMessage value : iterable) {
            //队列第一个元素是最新的元素
            if (StringUtils.isEmpty(gid)) {
                gid = value.getGid();
            }
            sum += value.getMoney();
        }

        final Tuple3<String, Double, String> result =
                new Tuple3<>(key, sum, gid);

        collector.collect(result);
    }
}
