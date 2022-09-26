package com.lz.demo.sink;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 创建jedis连接
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.56.3")
                .setPort(6379)
                .setPassword("123456")
                .build();

        stream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }
    }
}
