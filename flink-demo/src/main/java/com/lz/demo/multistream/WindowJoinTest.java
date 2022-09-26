package com.lz.demo.multistream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoinTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 4500L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                        Tuple3.of("a", "a", 3000L),
                        Tuple3.of("b", "b", 4000L),
                        Tuple3.of("c", "c", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
                );

        // window join
        stream1.join(stream2)
                .where(v1 -> v1.f0)
                .equalTo(v2 -> v2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply((v1, v2) -> v1 + " -> " + v2)
                .print();

        env.execute();
    }
}
