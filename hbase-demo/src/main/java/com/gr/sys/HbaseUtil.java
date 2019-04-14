package com.gr.sys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


/**
 * 一个不错的基于原生Hbase Api的工具类
 */
public class HbaseUtil {

    //@Value("${hbase.zookeeper.quorum}")
    private  String addr="HDP1,HDP2,HDP3";

    //@Value("${hbase.zookeeper.property.clientPort}")
    private  String port="2181";


    Logger logger = Logger.getLogger(getClass());

    private static Connection connection;

    @SuppressWarnings("static-access")
    public void getConnection(){
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum",addr);
        conf.set("hbase.zookeeper.property.clientPort", port);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HbaseUtil() {
        getConnection();
    }

    /*
     * 判断表是否存在
     *
     *
     * @tableName 表名
     */

    public boolean isExist(String tableName) throws IOException {
        TableName table_name = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        boolean exit = admin.tableExists(table_name);
        admin.close();
        return exit;
    }

    /**
     * 单行添加
     * @param tableName
     * @param rowKey
     * @param family
     * @param keyValue
     * @throws IOException
     */
    @SuppressWarnings("unused")
    private static void addRow(String tableName, String rowKey, String family, Map<String, String> keyValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Entry<String, String> entry : keyValue.entrySet()) {
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        table.put(put);
        table.close();
        keyValue.clear();
    }


    public static void addRows(String tableName, String rowFamilySeparator, Map<String, Map<String, String>> keyValues) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        for (Entry<String, Map<String, String>> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            if (null == rowFamilySeparator || rowFamilySeparator.isEmpty()) {
                rowFamilySeparator = "_";
            }
            String rowKey = key.split(rowFamilySeparator)[0];
            String family = key.split(rowFamilySeparator)[1];
            Map<String, String> keyValue = entry.getValue();
            Put put = new Put(Bytes.toBytes(rowKey), System.currentTimeMillis());
            for (Entry<String, String> entry2 : keyValue.entrySet()) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry2.getKey()), Bytes.toBytes(entry2.getValue()));
            }
            puts.add(put);
        }
        table.put(puts);
        table.close();
        keyValues.clear();
    }

    /**
     * 单行删除
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 查询所有
     * @param tableName
     * @param family
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> queryForScan(String tableName, String family) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                row = new HashMap<String, String>();
                for (Cell cell : cells) {
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                }
                rows.add(row);
            }
        } finally {
            rs.close();
        }
        return rows;
    }

    /**
     * 查询所有rowKey
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<String> queryAllRowKeyForScan(String tableName) throws IOException {
        List<String> result = new ArrayList<String>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                if (null == cells || cells.length <= 0) {
                    continue;
                }
                Cell cell = cells[0];
                String rowKey = new String(CellUtil.cloneRow(cell));
                result.add(rowKey);
            }
        } finally {
            rs.close();
        }
        return result;
    }

    /**
     * 查询所有字段
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> queryForScan(String tableName) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                row = new HashMap<String, String>();
                for (Cell cell : cells) {
                    row.put("rowKey", new String(CellUtil.cloneRow(cell)));
                    row.put("family", new String(CellUtil.cloneFamily(cell)));
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                }
                rows.add(row);
            }
        } finally {
            rs.close();
        }
        return rows;
    }

    /**
     * 根据时间范围
     * @param tableName
     * @param family
     * @param minStamp
     * @param maxStamp
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> queryForTimeRange(String tableName, String family, long minStamp, long maxStamp) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        scan.setTimeRange(minStamp, maxStamp);
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                row = new HashMap<String, String>();
                for (Cell cell : cells) {
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                }
                rows.add(row);
            }
        } finally {
            rs.close();
        }
        return rows;
    }

    /**
     * 根据RowKey查询
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     * @throws IOException
     */
    public static Map<String, String> queryForRowKey(String tableName, String rowKey, String family) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));
        Scan scan = new Scan(get);
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                row = new HashMap<String, String>();
                for (Cell cell : cells) {
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell),"UTF-8"));
                }
            }
        } finally {
            rs.close();
        }
        return row;
    }

    /**
     * 根据多个RowKey查询
     * @param tableName
     * @param rowKeys
     * @param family
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> queryForRowKeys(String tableName, List<String> rowKeys, String family) throws IOException {
        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> getList = new ArrayList();
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            getList.add(get);
        }
        Result[] results = table.get(getList);
        for (Result result : results){//对返回的结果集进行操作
            Map<String, String> row = new HashMap<String, String>();
            for (Cell kv : result.rawCells()) {
                row.put(new String(CellUtil.cloneQualifier(kv)), new String(CellUtil.cloneValue(kv),"UTF-8"));
            }
            resultList.add(row);
        }
        return resultList;
    }

    /**
     * 根据RowKey范围查询
     * @param tableName
     * @param family
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public static List<Map<String, String>> queryForRowKeyRange(String tableName, String family, String startRow, String stopRow) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                row = new HashMap<String, String>();
                for (Cell cell : cells) {
                    row.put("timestamp", cell.getTimestamp() + "");
                    row.put("rowKey", new String(CellUtil.cloneRow(cell)));
                    row.put("family", new String(CellUtil.cloneFamily(cell)));
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                }
                rows.add(row);
            }
        } finally {
            rs.close();
        }
        return rows;
    }

    /**
     * 根据指定列名匹配列值
     * @param tableName
     * @param family
     * @param value
     * @return
     * @throws IOException
     */
    public static Collection<Map<String, String>> queryForQuilfier(String tableName, String family, String column, String value) throws IOException {
        Map<String, Map<String, String>> rows = new HashMap<String, Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SubstringComparator comp = new SubstringComparator(value);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(family.getBytes(), column.getBytes(), CompareOp.EQUAL, comp);
        filter.setFilterIfMissing(true);
        PageFilter p = new PageFilter(5);
        scan.setFilter(filter);
        scan.setFilter(p);
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    if (null == row || !rows.containsKey(rowKey)) {
                        row = new HashMap<String, String>();
                    }
                    row.put("timestamp", cell.getTimestamp() + "");
                    row.put("rowKey", rowKey);
                    row.put("family", new String(CellUtil.cloneFamily(cell)));
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell),"UTF-8"));
                    rows.put(rowKey,row);
                }
            }
        } finally {
            rs.close();
        }
        return rows.values();
    }

    /**
     * 根据指定列名完全匹配列值
     * @param tableName
     * @param family
     * @param value
     * @return
     * @throws IOException
     */
    public static Collection<Map<String, String>> queryForQuilfierExactly(String tableName, String family, String column, String value) throws IOException {
        Map<String, Map<String, String>> rows = new HashMap<String, Map<String, String>>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(family.getBytes(), column.getBytes(), CompareOp.EQUAL,value.getBytes());
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        Map<String, String> row = null;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    if (null == row || !rows.containsKey(rowKey)) {
                        row = new HashMap<String, String>();
                    }
                    row.put("timestamp", cell.getTimestamp() + "");
                    row.put("rowKey", rowKey);
                    row.put("family", new String(CellUtil.cloneFamily(cell)));
                    row.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell),"UTF-8"));
                    rows.put(rowKey,row);
                }
            }
        } finally {
            rs.close();
        }
        return rows.values();
    }

    /**
     * 获取列名匹配的rowkey
     * @param tableName
     * @param qualifier 列名
     * @param pageSize 数量
     * @return
     * @throws IOException
     */
    public static List<String> queryForQuilfierName(String tableName, String qualifier, long pageSize) throws IOException {
        Set<String> rowKeys = new HashSet<String>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter filter = new QualifierFilter(CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(qualifier)));
        PageFilter pageFilter = new PageFilter(pageSize);
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);
        filterList.addFilter(pageFilter);
        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    rowKeys.add(rowKey);
                }
            }
        } finally {
            rs.close();
        }
        List<String> rows = new ArrayList<String>(rowKeys);
        return rows;
    }

    /**
     * 获取多个列值匹配的rowkey
     * @param tableName
     * @param family
     * @param qualifierMap 列名:列值
     * @param pageSize 数量
     * @return
     * @throws IOException
     */
    public static List<String> queryForMultiQuilfierName(String tableName, String family, Map<String, String> qualifierMap, long pageSize) throws IOException {
        Set<String> rowKeys = new HashSet<String>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        for (Entry<String, String> entry : qualifierMap.entrySet()) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(family.getBytes(), entry.getKey().getBytes(), CompareOp.EQUAL,entry.getValue().getBytes());
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
        }
        PageFilter pageFilter = new PageFilter(pageSize);
        filterList.addFilter(pageFilter);
        scan.setFilter(filterList);
        scan.addFamily(Bytes.toBytes(family));
        ResultScanner rs = table.getScanner(scan);
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    rowKeys.add(rowKey);
                }
            }
        } finally {
            rs.close();
        }
        List<String> rows = new ArrayList<String>(rowKeys);
        return rows;
    }

    public static void qualifierFilter() throws IOException {
        Table mTable = connection.getTable(TableName.valueOf("portrait"));
        QualifierFilter columnsNameFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("basictag-14".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(columnsNameFilter);
        ResultScanner rs = mTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            Cell[] cells = r.rawCells();
            for (Cell cell : cells) {
                System.out.println("==== "+new String(CellUtil.cloneRow(cell))+"\t"+
                        new String(CellUtil.cloneFamily(cell))+"\t"+
                        new String(CellUtil.cloneQualifier(cell))+"\t"+
                        new String(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取列名匹配的rowkey
     * @param tableName
     * @param qualifier 列名
     * @param pageSize 数量
     * @return
     * @throws IOException
     */
    public static List<String> queryForQuilfierName(String tableName, String family, String qualifier, long pageSize) throws IOException {
        Set<String> rowKeys = new HashSet<String>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(qualifier.getBytes()));
        PageFilter pageFilter = new PageFilter(pageSize);
        FilterList list = new FilterList();
        list.addFilter(pageFilter);
        list.addFilter(qualifierFilter);
        scan.addFamily(Bytes.toBytes(family));
        scan.setFilter(list);
        System.out.println(scan.toJSON());
        ResultScanner rs = table.getScanner(scan);
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    System.out.println("==== "+new String(CellUtil.cloneRow(cell))+"\t"+
                            new String(CellUtil.cloneFamily(cell))+"\t"+
                            new String(CellUtil.cloneQualifier(cell))+"\t"+
                            new String(CellUtil.cloneValue(cell)));
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    rowKeys.add(rowKey);
                }
            }
        } finally {
            rs.close();
        }
        List<String> rows = new ArrayList<String>(rowKeys);
        return rows;
    }

    /**
     * 根据指定列名匹配列值条数
     * @param tableName
     * @param family
     * @param value
     * @return
     * @throws IOException
     */
    public static long queryForQuilfierCount(String tableName, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SubstringComparator comp = new SubstringComparator(value);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(family.getBytes(), column.getBytes(), CompareOp.EQUAL, comp);
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        long count = 0;
        try {
            for (Result r = rs.next(); r != null; r = rs.next()) {
                count++;
            }
        } finally {
            rs.close();
        }
        return count;
    }

    /*
     * 关闭连接
     *
     */
    public void close() {
        /**
         * close connection
         **/
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
