package com.lz.demo.simple;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3.将每行数据进行分词，转换成二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            Arrays.stream(words).forEach(word -> out.collect(Tuple2.of(word, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5.分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 6.打印结果
        sum.print();
    }
}
