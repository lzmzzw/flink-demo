package com.lz.demo.multistream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // app支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                        Tuple3.of("order1", "app", 1000L),
                        Tuple3.of("order2", "app", 2000L),
                        Tuple3.of("order3", "app", 3500L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
                );

        // 第三方平台支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                        Tuple4.of("order1", "third-party", "success", 3000L),
                        Tuple4.of("order2", "third-party", "success", 4000L),
                        Tuple4.of("order4", "third-party", "success", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f3)
                );

        // 检测同一支付单在两条流中是否匹配
        // 以下两种写法等价
        // appStream.keyBy(v -> v.f0).connect(thirdPartyStream.keyBy(v -> v.f0));
        appStream.connect(thirdPartyStream).keyBy(v -> v.f0, v -> v.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    // 自定义实现CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，用来保存已到达事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyState;

        @Override
        public void open(Configuration parameters) {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            thirdPartyState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("third-party-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws IOException {
            // 来的是app event，查看另一条流中事件是否来过
            if (thirdPartyState.value() != null) {
                collector.collect("对账成功：" + value + " " + thirdPartyState.value());
                // 清空third party event状态
                thirdPartyState.clear();
            } else {
                // 更新app event状态
                appEventState.update(value);
                // 注册一个定时器，等待另一条流事件
                context.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws IOException {
            // 来的是third party event，查看另一条流中事件是否来过
            if (appEventState.value() != null) {
                collector.collect("对账成功：" + value + " " + appEventState.value());
                // 清空app event状态
                appEventState.clear();
            } else {
                // 更新third party event状态
                thirdPartyState.update(value);
                // 注册一个定时器，等待另一条流事件
                context.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，如果某个状态不为空，则表明另一条流中事件未到，对账失败
            if (appEventState.value() != null) {
                out.collect("对账失败： " + appEventState.value() + " 第三方支付信息未收到");
            }
            if (thirdPartyState.value() != null) {
                out.collect("对账失败： " + thirdPartyState.value() + " app支付信息未收到");
            }
            appEventState.clear();
            thirdPartyState.clear();
        }
    }
}
