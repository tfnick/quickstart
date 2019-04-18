package examples.streaming.antifraud.sink.redis.command;

import com.zhisheng.examples.streaming.antifraud.sink.redis.RedisCommand;
import redis.clients.jedis.Jedis;

/**
 * override rpush command
 */

public class RedisRPushCommand extends RedisCommand {

    public RedisRPushCommand(String key, Object value) {
        super(key, value);
    }

    public RedisRPushCommand(String key, Object value, int expire) {
        super(key, value, expire);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.rpush(getKey(), (String[]) getValue());
    }
}
