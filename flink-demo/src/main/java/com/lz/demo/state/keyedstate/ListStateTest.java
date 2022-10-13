package com.lz.demo.state.keyedstate;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ListStateTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // stream1
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                        Tuple3.of("order1", "app", 1000L),
                        Tuple3.of("order2", "app", 2000L),
                        Tuple3.of("order3", "app", 3500L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
                );

        // stream2
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> stream2 = env.fromElements(
                        Tuple4.of("order1", "third-party", "success", 3000L),
                        Tuple4.of("order2", "third-party", "success", 4000L),
                        Tuple4.of("order4", "third-party", "success", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f3)
                );

        // 自定义状态进行全外连接
        stream1.keyBy(v -> v.f0)
                .connect(stream2.keyBy(v -> v.f0))
                .process(new IntervalJoinResult())
                .print();

        env.execute();
    }

    // 自定义实现CoProcessFunction
    public static class IntervalJoinResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义列表状态，表示两条流中到达的所有数据
        private ListState<Tuple3<String, String, Long>> stream1ListState;
        private ListState<Tuple4<String, String, String, Long>> stream2ListState;

        @Override
        public void open(Configuration parameters) {
            stream1ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("stream1ListState", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            stream2ListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("stream2ListState", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            // 获取另一条流中所有数据，匹配输出
            for (Tuple4<String, String, String, Long> right : stream2ListState.get()) {
                collector.collect(left + " => " + right);
            }

            stream1ListState.add(left);
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            // 获取另一条流中所有数据，匹配输出
            for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                collector.collect(left + " => " + right);
            }

            stream2ListState.add(right);
        }
    }
}
