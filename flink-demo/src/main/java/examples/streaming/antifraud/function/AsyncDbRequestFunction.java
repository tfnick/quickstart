package examples.streaming.antifraud.function;

import akka.japi.tuple.Tuple3;
import ch.qos.logback.classic.db.names.TableName;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.assertj.core.util.Lists;
import org.hbase.async.*;

import java.util.ArrayList;

/**
 * 演示flink通过Async IO Function接口实现异步访问数据库或者Http服务
 */
public class AsyncDbRequestFunction extends RichAsyncFunction<String, Tuple3<String,Integer,Integer>> {

    //async hbase client The client is compatible with HBase from versions 0.92 to 0.94 and 0.96 to 1.3.
    HBaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Config config = new Config();
        config.getMap().put("hbase.zookeeper.quorum", "192.168.1.1:2181,192.168.1.2:2181");
        config.getMap().put("hbase.zookeeper.session.timeout", "5000");
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
    public void asyncInvoke(String idCard, ResultFuture<Tuple3<String, Integer, Integer>> resultFuture) throws Exception {
        //query table idcard(id,x2,x3)
        GetRequest getRequest = new GetRequest("gr_idcard", idCard, "df");

        client.get(getRequest).addCallback(new Callback<Tuple3<String, Integer, Integer>, ArrayList<KeyValue>>(){
            @Override
            public Tuple3<String, Integer, Integer> call(ArrayList<KeyValue> keyValues) throws Exception {
                Tuple3<String, Integer, Integer> retTuple = null;

                String idCard = null;
                Integer x2 = null;
                Integer x3 = null;
                if (keyValues != null) {
                    for (KeyValue kv : keyValues) {
                        switch (String.valueOf(kv.key())) {
                            case "id_card" :
                                idCard = String.valueOf(kv.value());break;
                            case "x2" :
                                x2 = Integer.valueOf(String.valueOf(kv.value()));break;
                            case "x3" :
                                x3 = Integer.valueOf(String.valueOf(kv.value()));break;
                            default:
                                break;
                        }
                    }
                }
                if (StringUtils.isNotEmpty(idCard)) {
                    retTuple = new Tuple3<>(idCard, x2, x3);
                    resultFuture.complete(Lists.newArrayList(retTuple));
                }
                //返回值用不到
                return retTuple;
            }
        });

    }
}
