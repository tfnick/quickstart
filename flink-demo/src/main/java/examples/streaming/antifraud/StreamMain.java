package examples.streaming.antifraud;

import com.gr.common.utils.JacksonUtil;
import examples.streaming.antifraud.function.ApplyCountAggregator;
import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.sink.mysql.MysqlApplyGroupSink;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMain.class);

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockMessageSource());

        applyStream.filter(new FilterFunction<ApplyMessage>() {
            @Override
            public boolean filter(ApplyMessage applyMessage) throws Exception {
                LOGGER.info("receive event {}", JacksonUtil.toJson(applyMessage));
                return applyMessage.getIdCard() != null && applyMessage.getMoney() > 0;
            }
        })
                .keyBy(new KeySelector<ApplyMessage, String>() {
                    @Override
                    public String getKey(ApplyMessage applyMessage) throws Exception {
                        return applyMessage.getIdCard();
                    }
                })
                .timeWindow(Time.seconds(10),Time.seconds(1))
                .aggregate(new ApplyCountAggregator()).print();
                /*
                .keyBy(new KeySelector<ApplyMessage, String>() {
                    @Override
                    public String getKey(ApplyMessage applyMessage) throws Exception {
                        return applyMessage.getIdCard();
                    }
                })
                .timeWindow(Time.seconds(10))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //.aggregate(new ApplyCountAggregator())
                .process(new CountProcessWindowFunction()).print();
                //.addSink(new MysqlApplyGroupSink());
                */

        env.execute("x");
    }
    
    static class CountProcessWindowFunction extends ProcessWindowFunction<ApplyMessage, Tuple2<String, Integer>, String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<ApplyMessage> input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            int idcard_mobile_count = 0;
            for (ApplyMessage element : input) {
                //count only not distinct
                idcard_mobile_count++;
            }
            collector.collect(Tuple2.of(key,idcard_mobile_count));
        }
    }
}
