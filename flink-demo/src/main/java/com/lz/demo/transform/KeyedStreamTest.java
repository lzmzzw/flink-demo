package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStreamTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Tom", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=1", 4000L),
                new Event("Tom", "/prod?id=1", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L)
        );

        // 转换计算
        // 1.使用自定义类，实现KeySelector接口
        SingleOutputStreamOperator<Event> result1 = stream.keyBy(new MyMapper()).max("timestamp");

        // 2.使用lambda表达式
        SingleOutputStreamOperator<Event> result2 = stream.keyBy(v -> v.user).maxBy("timestamp");

        result1.print("max");
        result2.print("maxBy");

        env.execute();
    }

    // 自定义map function
    public static class MyMapper implements KeySelector<Event, String> {
        @Override
        public String getKey(Event event) {
            return event.user;
        }
    }
}
