package com.lz.demo.window;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowAggregateWithProcessTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream.print();

        // 聚合
        SingleOutputStreamOperator<String> result = stream
                .keyBy(v -> v.user)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new PvUvAvgFunction(), new ProcessWindow());

        result.print();

        env.execute();
    }

    /**
     * pv 页面点击数
     * uv 用户
     * 本例计算用户平均点击次数
     */
    public static class PvUvAvgFunction implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
            accumulator.f1.add(event.user);
            return Tuple2.of(accumulator.f0 + 1L, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return (double) accumulator.f0 / accumulator.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> accumulator1, Tuple2<Long, HashSet<String>> accumulator2) {
            accumulator1.f1.addAll(accumulator2.f1);
            return Tuple2.of(accumulator1.f0 + accumulator2.f0, accumulator1.f1);
        }
    }

    /**
     * 包装窗口信息输出
     */
    public static class ProcessWindow extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Double, String, String, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> collector) {
            // 窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();

            collector.collect("窗口 " + new Timestamp(start) + " ~ " + new Timestamp(end) + " 平均点击次数为：" + elements.iterator().next());
        }
    }
}
