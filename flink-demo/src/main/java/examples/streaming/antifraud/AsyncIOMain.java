package examples.streaming.antifraud;

import examples.streaming.antifraud.model.ApplyMessage;
import examples.streaming.antifraud.source.MockMessageSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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



                //env.execute("x");

    }
}
