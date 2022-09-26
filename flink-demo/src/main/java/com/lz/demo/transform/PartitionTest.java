package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionTest {
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

        // 1.随机分区
        // stream.shuffle().print().setParallelism(4);

        // 2.轮询分区
        // stream.rebalance().print().setParallelism(4);

        // 3.重缩放分区
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> sourceContext) {
//                for (int i = 1; i <= 8; i++) {
//                    // 将奇偶数分别发送到1号和10号并行分区
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        sourceContext.collect(i);
//                    }
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(2)
//                .rescale()
//                .print()
//                .setParallelism(4);

        // 4.广播
        // stream.broadcast().print().setParallelism(4);

        // 5.全局分区
        // stream.global().print().setParallelism(4);

        // 6.自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom((Partitioner<Integer>) (key, numPartitions) -> key % 2, (KeySelector<Integer, Integer>) value -> value)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
