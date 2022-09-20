package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> steam = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L)
        );

        // 转换计算
        // 1.使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = steam.map(new MyMapper());

        // 2.使用lambda表达式
        SingleOutputStreamOperator<String> result2 = steam.map(v -> v.user);

        result1.print("1");
        result2.print("2");

        env.execute();
    }

    // 自定义map function
    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event event) {
            return event.user;
        }
    }
}
