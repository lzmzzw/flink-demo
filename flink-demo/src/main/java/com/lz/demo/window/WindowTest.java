package com.lz.demo.window;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=10", 4000L),
                new Event("Tom", "/prod?id=10", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L),
                new Event("Jerry", "/home", 8000L)
        );

        // 乱序流watermark生成
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
        );

        // 滚动事件时间窗口
        stream.keyBy(v -> v.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)));

        // 滑动事件时间窗口
        stream.keyBy(v -> v.user)
                .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)));

        // 会话事件时间窗口
        stream.keyBy(v -> v.user)
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)));

        // 计数窗口，传一个参数为滚动窗口，传两个参数为滑动窗口
        stream.keyBy(v -> v.user)
                .countWindow(10, 2);

        stream.print();

        env.execute();
    }
}
