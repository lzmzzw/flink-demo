package com.lz.demo.state.keyedstate;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class MapStateTest {
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

        stream.print("input");

        // 统计每个用户的pv
        stream.keyBy(v -> v.user)
                .process(new FakeWindowPvResult(10000L))
                .print();

        env.execute();
    }

    public static class FakeWindowPvResult extends KeyedProcessFunction<String, Event, String> {
        // 定义窗口大小
        private final long windowSize;
        // 定义MapState， 保存每个窗口中的统计值
        MapState<Long, Long> windowUrlCountMapState;

        public FakeWindowPvResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("windowCount", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
            long windowStart = event.timestamp / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册end - 1的定时器
            context.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态，进行增量聚合
            windowUrlCountMapState.put(windowStart, windowUrlCountMapState.contains(windowStart) ? windowUrlCountMapState.get(windowStart) + 1 : 1L);

            // 定时器触发时输出结果
//            context.timerService().registerEventTimeTimer(windowSize);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出统计结果
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;

            long count = windowUrlCountMapState.get(windowStart);

            out.collect("窗口" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + " user: " + ctx.getCurrentKey() + " count: " + count);
            // 模拟窗口关闭，清除map中对应key
            windowUrlCountMapState.remove(windowStart);
        }
    }

}
