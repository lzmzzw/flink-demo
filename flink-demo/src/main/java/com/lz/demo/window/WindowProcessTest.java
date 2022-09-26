package com.lz.demo.window;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class WindowProcessTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream.print();

        // 使用ProcessWindowFunction计算每个用户访问量
        SingleOutputStreamOperator<Map<String, Long>> result = stream
                .keyBy(v -> "key")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .process(new VvCountByWindow());

        result.print();

        env.execute();
    }

    public static class VvCountByWindow extends ProcessWindowFunction<Event, Map<String, Long>, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Event, Map<String, Long>, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Map<String, Long>> collector) {
            // 窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            System.out.println(new Timestamp(start) + " ~ " + new Timestamp(end));

            HashMap<String, Long> map = new HashMap<>();
            elements.forEach(v -> map.put(v.user, map.getOrDefault(v.user, 0L) + 1L));

            collector.collect(map);
        }
    }
}
