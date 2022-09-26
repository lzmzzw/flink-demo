package com.lz.demo.window;

import com.lz.demo.entity.Event;
import com.lz.demo.entity.UrlCount;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowAggregateUrlCount {
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
        SingleOutputStreamOperator<UrlCount> result = stream
                .keyBy(v -> v.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new UrlCountAggregate(), new UrlCountResult());

        result.print();

        env.execute();
    }

    /**
     * url访问次数
     */
    public static class UrlCountAggregate implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

    /**
     * 包装窗口信息输出
     */
    public static class UrlCountResult extends ProcessWindowFunction<Long, UrlCount, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Long, UrlCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlCount> collector) throws Exception {
            // 窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(new UrlCount(key, iterable.iterator().next(), start, end));
        }
    }
}
