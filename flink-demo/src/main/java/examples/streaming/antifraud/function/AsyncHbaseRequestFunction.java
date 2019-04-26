package examples.streaming.antifraud.function;

import akka.japi.tuple.Tuple3;
import ch.qos.logback.classic.db.names.TableName;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import examples.streaming.antifraud.model.ApplyMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.assertj.core.util.Lists;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * 演示flink通过Async IO Function接口实现异步访问数据库或者Http服务
 */
public class AsyncHbaseRequestFunction extends RichAsyncFunction<ApplyMessage, Tuple3<String,Integer,Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncHbaseRequestFunction.class);

    //async hbase client The client is compatible with HBase from versions 0.92 to 0.94 and 0.96 to 1.3.
    HBaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Config config = new Config();

        config.overrideConfig("hbase.zookeeper.quorum", "192.168.1.168:2181");
        config.overrideConfig("hbase.zookeeper.session.timeout", "5000");
        client = new HBaseClient(config);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public void asyncInvoke(ApplyMessage applyMessage, ResultFuture<Tuple3<String, Integer, Integer>> resultFuture) throws Exception {
        LOGGER.info("query id_card table by rowkey {}", applyMessage.getIdCard());

        //query table idcard(id,x2,x3)
        GetRequest getRequest = new GetRequest("id_card", applyMessage.getIdCard(), "df");

        client.get(getRequest).addCallback(new Callback<Tuple3<String, Integer, Integer>, ArrayList<KeyValue>>(){
            @Override
            public Tuple3<String, Integer, Integer> call(ArrayList<KeyValue> keyValues) throws Exception {
                Tuple3<String, Integer, Integer> retTuple = null;

                String rowKeyIdCard = null;
                Integer x2 = null;
                Integer x3 = null;
                if (keyValues != null) {
                    LOGGER.info("get result size is {}", keyValues.size());
                    for (KeyValue kv : keyValues) {
                        LOGGER.info("Key {} Column {} Value {}", new String(kv.key()), new String(kv.qualifier()),new String(kv.value()));

                        if(StringUtils.isEmpty(rowKeyIdCard)){//rowKey
                            rowKeyIdCard = new String(kv.key());
                        }
                        switch (new String(kv.qualifier())) {//column
                            case "x2" :
                                x2 = Integer.valueOf(new String(kv.value()));break;//column value
                            case "x3" :
                                x3 = Integer.valueOf(new String(kv.value()));break;
                            default:
                                break;
                        }
                    }
                }
                if (StringUtils.isNotEmpty(rowKeyIdCard)) {
                    retTuple = new Tuple3<>(rowKeyIdCard, x2, x3);
                    resultFuture.complete(Lists.newArrayList(retTuple));
                }else{
                    retTuple = new Tuple3<>(applyMessage.getIdCard(), -1, -1);
                    resultFuture.complete(Lists.newArrayList(retTuple));
                }
                //返回值用不到
                return retTuple;
            }
        });

    }
}
