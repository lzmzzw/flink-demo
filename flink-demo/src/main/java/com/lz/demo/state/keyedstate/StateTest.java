package com.lz.demo.state.keyedstate;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
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

        stream.keyBy(v -> v.user)
                .flatMap(new MyFlatMap()).print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;

        // 定义本地变量
        Long count = 0L;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("my-map", String.class, Long.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("my-reducing", (ReduceFunction<Event>) (event1, event2) -> new Event(event1.user, event1.url, event2.timestamp), Event.class));
            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("my-aggregating", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event event, Long accumulator) {
                    return accumulator + 1;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "count: " + accumulator;
                }

                @Override
                public Long merge(Long accumulator1, Long accumulator2) {
                    return accumulator1 + accumulator2;
                }
            }, Long.class));

            // 配置状态的ttl
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(1))
                    // 读写时更新ttl，默认仅写时更新
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    // 失效但未清理的数据允许返回，默认不返回
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            // 访问和更新状态
            myValueState.update(event);
            System.out.println("myValueState: " + myValueState.value());

            myListState.add(event);
            System.out.println("myListState: " + myListState.get());

            myMapState.put(event.user, myMapState.get(event.user) == null ? 1 : myMapState.get(event.user) + 1);
            System.out.println("myMapState: " + myMapState.get(event.user));

            myAggregatingState.add(event);
            System.out.println("myAggregatingState: " + myAggregatingState.get());

            myReducingState.add(event);
            System.out.println("myReducingState: " + myReducingState.get());

            System.out.println("\n=============================== " + "count:" + count++ + " ===============================\n");
        }
    }
}
