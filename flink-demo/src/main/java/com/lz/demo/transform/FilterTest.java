package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L)
        );

        // 转换计算
        // 1.使用自定义类，实现FilterFunction接口
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyMapper());

        // 2.使用lambda表达式
        SingleOutputStreamOperator<Event> result2 = stream.filter(v -> v.user.equals("Tom"));

        result1.print("1");
        result2.print("2");

        env.execute();
    }

    // 自定义filter function
    public static class MyMapper implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) {
            return event.user.equals("Tom");
        }
    }
}
