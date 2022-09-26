package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=1", 4000L),
                new Event("Tom", "/prod?id=1", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L)
        );

        // 转换计算
        // 1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream
                .map(v -> Tuple2.of(v.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(v -> v.f0)
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1));

        // 2.选取当前最活跃用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser
                .keyBy(v -> "key")
                .reduce((v1, v2) -> v1.f1 > v2.f1 ? v1 : v2);

        result.print();

        env.execute();
    }
}
