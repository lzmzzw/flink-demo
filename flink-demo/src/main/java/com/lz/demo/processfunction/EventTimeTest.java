package com.lz.demo.processfunction;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从源读取
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        stream.keyBy(v -> v.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) {
                        long currentTime = context.timestamp();
                        collector.collect(context.getCurrentKey() + " 数据到达时间：" + new Timestamp(currentTime) + " watermark：" + context.timerService().currentWatermark());

                        // 注册一个10秒的定时器
                        context.timerService().registerEventTimeTimer(currentTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector) {
                        collector.collect(context.getCurrentKey() + " 定时器触发时间：" + new Timestamp(timestamp) + " watermark：" + context.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }
}
