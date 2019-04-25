package examples.streaming.antifraud;

import examples.streaming.antifraud.function.AsyncHbaseRequestFunction;
import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

/**
 * Use AsyncIO to enrich data attributes
 */
public class AsyncIOMain {

        //https://www.liangzl.com/get-article-detail-114587.html
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockMessageSource());

        //110123199010025771 (idCard)
        applyStream.map(new MapFunction<ApplyMessage, Tuple4<String, Integer, Integer, Double>>() {
            @Override
            public Tuple4<String, Integer, Integer, Double> map(ApplyMessage applyMessage) throws Exception {
                String idCard = applyMessage.getIdCard();
                Double money = applyMessage.getMoney();


                return null;
            }
        });

                //env.execute("x");

    }
}
