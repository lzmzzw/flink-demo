package com.lz.demo.state.keyedstate;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ValueStateTest {
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
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        // 定义状态，保存当前pv统计值，以及是否有定时器
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTsState", Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，更新对应count值
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            // 如果没有注册定时器，注册定时器
            if (timerTsState.value() == null) {
                context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                timerTsState.update(event.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出统计结果
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
            // 清空状态
            timerTsState.clear();
        }
    }

}
