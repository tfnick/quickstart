package examples.streaming.antifraud.source;

import examples.streaming.antifraud.model.ApplyMessage;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.UUID;

public class MockNoParalleSource implements SourceFunction<ApplyMessage> {

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<ApplyMessage> ctx) throws Exception {
        while(isRunning){
            //图书的排行榜
            ApplyMessage applyMessage = new ApplyMessage();

            applyMessage.setGid(UUID.randomUUID().toString());
            applyMessage.setApplyTime(new Date());
            applyMessage.setIdCard("" + 11012319901002577L + RandomUtils.nextInt(0, 9));
            applyMessage.setIp("192.168.1." + RandomUtils.nextInt(0, 224));
            applyMessage.setMobile("1380013800" + RandomUtils.nextInt(0, 9));
            applyMessage.setMoney(RandomUtils.nextDouble(1000, 5000));
            ctx.collect(applyMessage);

            //每1秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
