package examples.streaming.wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ComplexMain {

    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        DataStreamSource dss1 =  env.fromElements(WORDS1);
        DataStreamSource dss2 =  env.fromElements(WORDS2);

        dss1.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.toLowerCase().split("\\W+");

                for (String split : splits) {
                    if (split.length() > 0) {
                        out.collect(new Tuple2<>(split.toLowerCase().trim(), 1));
                    }
                }
            }
        })
        .keyBy(0)
        .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value1.f1);
            }
        }).join(
            dss2.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    String[] splits = value.toLowerCase().split("\\W+");

                    for (String split : splits) {
                        if (split.length() > 0) {
                            out.collect(new Tuple2<>(split.toLowerCase().trim(), 1));
                        }
                    }
                }
            })
                    .keyBy(0)
                    .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                            return new Tuple2<>(value1.f0, value1.f1 + value1.f1);
                        }
                    })
                    .filter(new FilterFunction<Tuple2<String, Integer>>() {
                        @Override
                        public boolean filter(Tuple2<String, Integer> o) throws Exception {
                            if (o.f1 > 1) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    })
            );




        //Streaming 程序必须加这个才能启动程序，否则不会有结果
        System.out.println(env.getExecutionPlan());
        //env.execute("zhisheng —— word count streaming demo");
    }



    private static final String[] WORDS1 = new String[]{
            "Person Life is short",
            "Life is beautiful"
    };

    private static final String[] WORDS2 = new String[]{
            "To be",
            "or not to be"
    };
}
