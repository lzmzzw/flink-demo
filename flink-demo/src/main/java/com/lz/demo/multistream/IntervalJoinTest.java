package com.lz.demo.multistream;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                        Tuple2.of("Tom", 1000L),
                        Tuple2.of("Jerry", 2000L),
                        Tuple2.of("Tom", 3000L),
                        Tuple2.of("Spike", 4500L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                        new Event("Tom", "/home", 1000L),
                        new Event("Jerry", "/cart", 3000L),
                        new Event("Jerry", "/prod?id=1", 5000L),
                        new Event("Spike", "/prod?id=10", 7000L),
                        new Event("Tom", "/prod?id=10", 15000L),
                        new Event("Tom", "/cart", 30000L),
                        new Event("Jerry", "/home", 50000L),
                        new Event("Jerry", "/home", 80000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        // interval join
        orderStream
                .keyBy(v -> v.f0)
                .intervalJoin(clickStream.keyBy(v -> v.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(right + " => " + left);
                    }
                })
                .print();

        env.execute();
    }
}
