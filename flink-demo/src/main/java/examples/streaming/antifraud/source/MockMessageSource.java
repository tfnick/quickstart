package examples.streaming.antifraud.source;

import examples.streaming.antifraud.model.ApplyMessage;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.UUID;

public class MockMessageSource implements SourceFunction<ApplyMessage> {

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<ApplyMessage> ctx) throws Exception {
        String cids[] = new String[]{"1","2"};
        while(isRunning){
            ApplyMessage applyMessage = new ApplyMessage();

            applyMessage.setGid(UUID.randomUUID().toString());
            applyMessage.setCid(cids[RandomUtils.nextInt(0, 2)]);
            applyMessage.setApplyTime(new Date());
            applyMessage.setIdCard("" + 1008L + RandomUtils.nextInt(0, 9));
            applyMessage.setIp("192.168.1." + RandomUtils.nextInt(0, 224));
            applyMessage.setMobile("1380013800" + RandomUtils.nextInt(0, 9));
            applyMessage.setMoney(RandomUtils.nextDouble(1000, 5000));
            ctx.collect(applyMessage);

            //每1秒产生一条数据
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
