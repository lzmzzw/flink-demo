package com.lz.demo.multistream;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("192.168.56.3", 7777)
                .map(v -> {
                    String[] fields = v.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream1.print("1");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("192.168.56.3", 9999)
                .map(v -> {
                    String[] fields = v.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream2.print("2");

        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect("水位线: " + context.timerService().currentWatermark());
                    }
                })
                .print("3");

        env.execute();
    }
}
