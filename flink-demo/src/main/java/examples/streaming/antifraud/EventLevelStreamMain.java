package examples.streaming.antifraud;

import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
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
        applyStream.keyBy("idCard")
                .timeWindow(Time.hours(1), Time.seconds(1));

    }
}