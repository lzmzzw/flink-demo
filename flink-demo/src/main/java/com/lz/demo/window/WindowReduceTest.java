package com.lz.demo.window;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowReduceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream.print();

        // 统计每个用户访问次数
        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream
                .map(v -> Tuple2.of(v.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(v -> v.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1));

        result.print();

        env.execute();
    }
}
