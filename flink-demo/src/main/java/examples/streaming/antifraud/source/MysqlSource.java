package examples.streaming.antifraud.source;

import examples.streaming.antifraud.ConfigKeys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlSource extends RichSourceFunction<Tuple2<String,Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSource.class);

    private Connection connection = null;
    private PreparedStatement ps = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(ConfigKeys.DRIVER_CLASS);//加载数据库驱动
        connection = DriverManager.getConnection(ConfigKeys.SOURCE_DRIVER_URL, ConfigKeys.SOURCE_USER, ConfigKeys.SOURCE_PASSWORD);//获取连接
        ps = connection.prepareStatement(ConfigKeys.SOURCE_SQL);

        super.open(parameters);
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String mobile = resultSet.getString("mobile");
                Integer hit = resultSet.getInt("hit");

                Tuple2<String,Integer> tuple2 = new Tuple2<>();
                tuple2.setFields(mobile,hit);

                sourceContext.collect(tuple2);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            LOGGER.error("runException:{}", e);
        }

    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            LOGGER.error("runException:{}", e);
        }
    }
}
