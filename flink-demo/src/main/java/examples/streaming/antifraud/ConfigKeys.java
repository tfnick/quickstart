package examples.streaming.antifraud;

public interface ConfigKeys {

    String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    String SINK_DRIVER_URL = "jdbc:mysql://mysql.gr-data.uat:3306/gr_flink_test?autoReconnect=true&useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull&failOverReadOnly=false";
    String SINK_USER = "root";
    String SINK_PASSWORD = "gr2758";

    String SOURCE_DRIVER_URL = "jdbc:mysql://mysql.gr-data.uat:3306/gr_flink_test?autoReconnect=true&useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull&failOverReadOnly=false";
    String SOURCE_USER = "root";
    String SOURCE_PASSWORD = "gr2758";

    //SQL define
    String SOURCE_SQL = "select mobile,hit from bl_source";
    String SINK_SQL = "insert into bl_sink (mobile,hit,ins_time) values (?,?,now())";

    String APPLY_GROUP_SINK_SQL = "insert into gr_apply_group_sink (group_key,metric,val,ins_time) values (?,?,?,now())";
    String APPLY_SINK_SQL = "insert into gr_apply_sink (gid,metric,val,ins_time) values (?,?,?,now())";
}
