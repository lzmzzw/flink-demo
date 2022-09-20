package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapTest {
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
        // 1.使用自定义类，实现FlatMapFunction接口
        SingleOutputStreamOperator<String> result1 = steam.flatMap(new MyMapper());

        // 2.使用lambda表达式
        SingleOutputStreamOperator<String> result2 = steam.flatMap((Event event, Collector<String> collector) -> {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }).returns(new TypeHint<String>() {
        });

        result1.print("1");
        result2.print("2");

        env.execute();
    }

    // 自定义flatmap function
    public static class MyMapper implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
