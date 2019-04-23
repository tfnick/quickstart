package examples.streaming.antifraud.source;

import examples.streaming.antifraud.model.HttpBlacklistResponse;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class HttpBlacklistSource extends RichSourceFunction<HttpBlacklistResponse> {

    /**
     * http入参
     */
    String ip;
    String idCard;
    String mobile;

    //构造时提供入参
    public HttpBlacklistSource(String ip, String idCard, String mobile) {
        this.ip = ip;
        this.idCard = idCard;
        this.mobile = mobile;
    }

    @Override
    public void run(SourceContext<HttpBlacklistResponse> cxt) throws Exception {
        //async I/O --- async http client
        //String result = httpClient.request(ip,idCard,mobile);

        Thread.sleep(100);

        HttpBlacklistResponse response = new HttpBlacklistResponse();

        response.setIdCardLevel(1);
        response.setIpLevel(2);
        response.setMobileLevel(-1);

        cxt.collect(response);
    }

    @Override
    public void cancel() {
        System.out.println("cancel source invoke");
        /*
        if (httpClient != null) {
            httpClient.clost();
        }
        */
    }
}
