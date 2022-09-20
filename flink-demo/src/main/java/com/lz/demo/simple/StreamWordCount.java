package com.lz.demo.simple;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在Program arguments中向main方法传入参数： --host 192.168.56.3 --port 9999
        // ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // String host = parameterTool.get("host");
        // int port = parameterTool.getInt("port");

        String host = "192.168.56.3";
        int port = 9999;

        // 2.读取socket流
        DataStreamSource<String> lineDataSteam = env.socketTextStream(host, port);

        // 3.将每行数据进行分词，转换成二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordTuple = lineDataSteam.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            Arrays.stream(words).forEach(word -> out.collect(Tuple2.of(word, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.按照word进行分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordTuple.keyBy(data -> data.f0);

        // 5.分组内进行聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6.打印结果
        sum.print();

        // 7.启动执行
        env.execute();
    }
}
