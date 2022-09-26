package com.lz.demo.multistream;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=10", 4000L),
                new Event("Tom", "/prod?id=10", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L),
                new Event("Jerry", "/home", 8000L)
        );

        // 乱序流watermark生成
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
        );

        // 定义侧输出流标签
        OutputTag<Tuple3<String, String, Long>> tom = new OutputTag<>("Tom", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));
        OutputTag<Tuple3<String, String, Long>> jerry = new OutputTag<>("Jerry", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));


        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) {
                if (event.user.equals("Tom")) {
                    context.output(tom, new Tuple3<>(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Jerry")) {
                    context.output(jerry, new Tuple3<>(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });

        processedStream.print("else");
        processedStream.getSideOutput(tom).print("Tom");
        processedStream.getSideOutput(jerry).print("Jerry");

        env.execute();
    }
}
