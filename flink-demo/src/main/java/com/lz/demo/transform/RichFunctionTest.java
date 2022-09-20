package com.lz.demo.transform;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> steam = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=10", 4000L),
                new Event("Tom", "/prod?id=10", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L)
        );

        // 转换计算
        SingleOutputStreamOperator<Integer> result = steam.map(new MyRichMapper()).setParallelism(2);

        result.print();

        env.execute();
    }

    // 实现自定义富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration configuration) throws Exception {
            super.open(configuration);
            System.out.println("open: " + getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Integer map(Event event) {
            return event.url.length();
        }

        @Override
        public void close() {
            System.out.println("close: " + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
