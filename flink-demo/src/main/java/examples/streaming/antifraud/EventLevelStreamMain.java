package examples.streaming.antifraud;

import examples.streaming.antifraud.function.ApplyCountAggregator;
import examples.streaming.antifraud.function.WindowCountApplyFunction;
import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import examples.streaming.antifraud.window.EventWindowAssigner;
import examples.streaming.antifraud.window.EventWindowTrigger;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLevelStreamMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMain.class);

    /**
     * 演示事件级别指标聚合计算
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        /**
         *  事件触发窗口聚合
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockMessageSource());

        //idCard关联的手机号数目
        SingleOutputStreamOperator<Tuple3<String,Double,String>> x1 = applyStream.keyBy("idCard")
                        .window(EventWindowAssigner.of(Time.seconds(3)))
                        .trigger(new EventWindowTrigger())
                        .apply(new WindowFunction<ApplyMessage, Tuple3<String, Double, String>, Tuple, TimeWindow>() {
                            @Override
                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ApplyMessage> iterable, Collector<Tuple3<String, Double, String>> collector) throws Exception {
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
                        });

        x1.print();

        LOGGER.info("--------------------------------------------------------------");

        env.execute("x");

    }
}