package com.lz.demo.multistream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Integer> stream1 = env.fromElements(1, 3, 5, 7);

        SingleOutputStreamOperator<Long> stream2 = env.fromElements(2L, 4L, 6L);

        // 合并两条流
        stream1.connect(stream2)
                .map(new CoMapFunction<Integer, Long, String>() {
                    @Override
                    public String map1(Integer integer) {
                        return integer.toString();
                    }

                    @Override
                    public String map2(Long aLong) {
                        return aLong.toString();
                    }
                })
                .print();

        env.execute();
    }
}
