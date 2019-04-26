package examples.streaming.antifraud;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class TableMain {

    /**
     * 使用table sql实现对有界数据集的查询
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableSource csvSource = new CsvTableSource("D:/GR_SOURCES/quickstart/flink-demo/src/main/resources/bl_mobile.csv",
                new String[]{"mobile","is_black","ins_time"},
                new TypeInformation[]{TypeInformation.of(String.class),TypeInformation.of(Integer.class),TypeInformation.of(String.class)});

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerTableSource("CsvTable", csvSource);

        /**table api**/
        //Table mobileBlTable = tableEnv.scan("CsvTable");
        //mobileBlTable.filter("mobile === '13800138002'").select("mobile,is_black as hitBlack");

        /**table sql**/
        Table table = tableEnv.sqlQuery("select mobile, is_black as hitBlack from CsvTable where mobile = '13800138002' ");

        DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

        dsRow.print();

        env.execute("x");
    }
}
