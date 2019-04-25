package com.gr.sys;

import com.gr.common.utils.JacksonUtil;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * http://hbased.gr-data.uat:16010
 */
public class HbaseUtilTest {


    @Test
    public void testCreate() throws Exception{
        Boolean bool = HbaseUtil.create("id_card", "df");
        Assert.assertTrue(bool);
    }

    @Test
    public void testPut() throws Exception{
        Map<String, Map<String, String>> rows = new HashMap<>();
        Map<String, String> row1 = new HashMap<>();
        row1.put("x2", String.valueOf(5));
        row1.put("x3", String.valueOf(6));
        Map<String, String> row2 = new HashMap<>();
        row2.put("x2", String.valueOf(1));
        row2.put("x3", String.valueOf(3));

        String rowKey1AndFamily = "10088" + "_" + "df";
        String rowKey2AndFamily = "10089" + "_" + "df";

        rows.put(rowKey1AndFamily, row1);
        rows.put(rowKey2AndFamily, row2);

        HbaseUtil.addRows("id_card", "_", rows);
    }

    @Test
    public void testQuery() throws Exception{
        Map<String,String> row = HbaseUtil.queryForRowKey("id_card", "10086", "df");
        Assert.assertNotNull(row);
        System.out.println(JacksonUtil.toJson(row));
    }

    @Test
    public void testQueryMulti() throws Exception{
        List<String> rowKeys = Lists.newArrayList("10086", "10087", "10088", "10089");
        List<Map<String,String>> rows = HbaseUtil.queryForRowKeys("id_card", rowKeys, "df");
        Assert.assertNotNull(rows);
        System.out.println(JacksonUtil.toJson(rows));
    }
}
