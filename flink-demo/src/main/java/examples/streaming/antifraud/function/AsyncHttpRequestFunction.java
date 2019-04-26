package examples.streaming.antifraud.function;

import akka.japi.tuple.Tuple3;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gr.common.utils.BasicTypeHelper;
import com.gr.common.utils.JacksonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 演示flink通过Async IO Function接口实现异步访问数据库或者Http服务
 */
public class AsyncHttpRequestFunction extends RichAsyncFunction<Tuple3<String,Integer,Integer>, Tuple3<String,Integer,Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncHttpRequestFunction.class);

    CloseableHttpAsyncClient httpclient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpclient = HttpAsyncClients.createDefault();
        httpclient.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (httpclient != null) {
            httpclient.close();
        }
    }

    @Override
    public void asyncInvoke(Tuple3<String,Integer,Integer> input, ResultFuture<Tuple3<String, Integer, Integer>> resultFuture) throws Exception {
        //{ "code" : 200, "message" : "OK", "data" : { "id" : "10086", "x2" : 5,"x3":6}}
        final HttpPost request = new HttpPost("http://www.mockhttp.cn/mock/gr/model");

        request.setHeader("Content-type", "application/json; charset=UTF-8");
        Map<String, Object> params = Maps.newHashMap();
        params.put("idCard", input.t1());

        StringEntity entity = new StringEntity(JacksonUtil.toJson(params));
        request.setEntity(entity);

        httpclient.execute(request, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse response) {
                try {
                    String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");

                    Map<String, Object> iResult = JacksonUtil.json2Object(responseBody, Map.class);
                    Integer code = BasicTypeHelper.getInteger(iResult, "code");
                    Integer x2 = BasicTypeHelper.getInteger(iResult, "data.score");

                    Tuple3<String, Integer, Integer> attrs = new Tuple3<>(input.t1(), x2, -1);

                    resultFuture.complete(Lists.newArrayList(attrs));
                } catch (Exception e) {

                }
            }

            @Override
            public void failed(Exception ex) {
                LOGGER.error("e", ex);
            }

            @Override
            public void cancelled() {
                LOGGER.error("request is cancelled");
            }
        });
    }
}
