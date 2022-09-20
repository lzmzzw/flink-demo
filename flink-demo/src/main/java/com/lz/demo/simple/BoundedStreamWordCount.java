package com.lz.demo.simple;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件读取数据
        DataStreamSource<String> lineDataSteamSource = env.readTextFile("input/words.txt");

        // 3.将每行数据进行分词，转换成二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataSteamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            Arrays.stream(words).forEach(word -> out.collect(Tuple2.of(word, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.按照word进行分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5.分组内进行聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6.打印结果
        sum.print();

        // 7.启动执行
        env.execute();
    }
}
