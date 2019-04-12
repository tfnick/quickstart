package com.gr.sys;

/**
 * 模拟flink流式处理完成向redis中写入指定gid的反欺诈结果
 */
public class FlinkWorkDoneMock {

    public static void main(String[] args) {
        String gid = "e3e92f40-eb72-48a3-8b81-d6b38f089aa5";

        String json = "{\"x1\": 1024,\"x2\": 8}";

        RedisUtil.lpush(gid, json);

        System.out.println("set value to redis done");

        System.exit(0);//distroy redis pool
    }
}
