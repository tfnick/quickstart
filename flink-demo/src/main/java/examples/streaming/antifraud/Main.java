package examples.streaming.antifraud;

import com.zhisheng.examples.streaming.antifraud.model.ApplyMessage;
import com.zhisheng.examples.streaming.antifraud.source.MockNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        DataStreamSource<ApplyMessage> applyStream = env.addSource(new MockNoParalleSource());
        //DataStreamSource<ApplyMessage> blackStream = env.addSource(new HttpBlacklistSource());


        applyStream.filter(new FilterFunction<ApplyMessage>() {
            @Override
            public boolean filter(ApplyMessage applyMessage) throws Exception {
                return applyMessage.getIdCard() != null && applyMessage.getMoney() > 0;
            }
        }).print();

        env.execute("x");
    }
}
