package com.lz.demo.window;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

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

        // 有序流watermark生成
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
//        );

        // 乱序流watermark生成
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
//        );

        // 自定义watermark
        stream.assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

        stream.print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp;
        }
    }

    // 周期式生成水位线
    public static class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {
        // 延迟时间
        private final long delayTime = 1000L;

        // 观察到的最大时间
        private long maxTs = -Long.MAX_VALUE + delayTime + 1L;

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 每来一条数据就调用一次
            // 更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 发射水位线，默认200ms调用一次
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    // 断点式生成水位线
    public static class PunctuatedGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 遇到特定的user，才发出水位线
            if (event.user.equals("Tom")) {
                watermarkOutput.emitWatermark(new Watermark(event.timestamp - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 不需要做任何事情，因为已经在onEvent中发射了水位线
        }
    }
}
