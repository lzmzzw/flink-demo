package com.lz.demo.window;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream.print();

        // 聚合
        SingleOutputStreamOperator<String> result = stream
                .keyBy(v -> v.user)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> accumulator) {
                        return new Timestamp(accumulator.f0 / accumulator.f1).toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> accumulator1, Tuple2<Long, Integer> accumulator2) {
                        return Tuple2.of(accumulator1.f0 + accumulator2.f0, accumulator1.f1 + accumulator2.f1);
                    }
                });

        result.print();

        env.execute();
    }
}
