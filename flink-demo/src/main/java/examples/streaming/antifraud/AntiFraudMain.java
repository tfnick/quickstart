package examples.streaming.antifraud;

import com.gr.common.utils.JacksonUtil;
import examples.streaming.antifraud.function.ApplyCountAggregator;
import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全流程反欺诈流处理演示
 * TODO
 * 流处理吞吐量的瓶颈在于外部IO请求
 * 可以考虑把前半部分的流式指标计算与后半部分的请求外部库补充维度信息以及调用模型和规则分开
 */
public class AntiFraudMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMain.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));


        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockMessageSource());

        SingleOutputStreamOperator<ApplyMessage> filterdStream = applyStream.filter(new FilterFunction<ApplyMessage>() {
            @Override
            public boolean filter(ApplyMessage applyMessage) throws Exception {
                if (applyMessage.getEt() == null) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        //DataStream<Object> ipX1Stream = filterdStream.keyBy("ip");

        /*
        applyStream
                .keyBy(new KeySelector<ApplyMessage, String>() {
                    @Override
                    public String getKey(ApplyMessage applyMessage) throws Exception {
                        return applyMessage.getIdCard();
                    }
                })
                .timeWindow(Time.seconds(10),Time.seconds(1))
                .aggregate(new ApplyCountAggregator()).print();
                */

        env.execute("x");
    }
}
