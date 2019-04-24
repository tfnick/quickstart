package examples.streaming.antifraud.sink.mysql;

import examples.streaming.antifraud.ConfigKeys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<Tuple2<String, Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSink.class);

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(ConfigKeys.DRIVER_CLASS);
        // 获取数据库连接
        connection = DriverManager.getConnection(ConfigKeys.SINK_DRIVER_URL,ConfigKeys.SINK_USER,ConfigKeys.SINK_PASSWORD);//写入mysql数据库
        preparedStatement = connection.prepareStatement(ConfigKeys.SINK_SQL);//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        try {
            String mobile = value.f0;
            Integer hit = value.f1;
            preparedStatement.setString(1, mobile);
            preparedStatement.setInt(2, hit);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            LOGGER.error("error", e);
        }
    }
}
