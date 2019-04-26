package examples.streaming.antifraud;

import akka.japi.tuple.Tuple3;
import examples.streaming.antifraud.function.AsyncHbaseRequestFunction;
import examples.streaming.antifraud.function.AsyncHttpRequestFunction;
import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Use AsyncIO to enrich data attributes
 */
public class AsyncIOMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockMessageSource());

        AsyncHbaseRequestFunction hbaseRequestFunction = new AsyncHbaseRequestFunction();
        AsyncHttpRequestFunction httpRequestFunction = new AsyncHttpRequestFunction();

        //模拟访问数据库
        DataStream<Tuple3<String,Integer,Integer>> ordered1Result = AsyncDataStream.orderedWait(applyStream,hbaseRequestFunction,1000, TimeUnit.MILLISECONDS);
        //模拟访问http模型接口（后续可以改为PMML）
        DataStream<Tuple3<String, Integer, Integer>> ordered2Result = AsyncDataStream.orderedWait(ordered1Result, httpRequestFunction, 50, TimeUnit.MILLISECONDS);

        ordered2Result.print();

        env.execute("x");

    }
}
