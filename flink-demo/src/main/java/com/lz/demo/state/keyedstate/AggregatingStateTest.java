package com.lz.demo.state.keyedstate;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AggregatingStateTest {
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

        // 统计每个用户的点击平均时间戳
        stream.keyBy(v -> v.user)
                .flatMap(new AvgTsResult(5))
                .print();

        env.execute();
    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        private final long count;
        // 定义一个聚合状态，用于保存平均时间戳
        AggregatingState<Event, Long> avgTsAggState;
        // 定义一个值状态，用于保存用户访问的次数
        ValueState<Long> countState;

        public AvgTsResult(long count) {
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            avgTsAggState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<>(
                            "avg",
                            new MyAggregate(),
                            Types.TUPLE(Types.LONG, Types.LONG)));
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            // 每来一条数据，count 加一
            countState.update(countState.value() == null ? 1L : countState.value() + 1);
            avgTsAggState.add(event);

            // 达到count次数，输出结果
            if (countState.value().equals(count)) {
                collector.collect(event.user + " 过去 " + count + " 次访问平均时间戳为 " + avgTsAggState.get());
                // 清理状态
                countState.clear();
                avgTsAggState.clear();
            }
        }
    }

    public static class MyAggregate implements AggregateFunction<Event, Tuple2<Long, Long>, Long> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> accumulator) {
            return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1);
        }

        @Override
        public Long getResult(Tuple2<Long, Long> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> accumulator1, Tuple2<Long, Long> accumulator2) {
            return null;
        }
    }

}
