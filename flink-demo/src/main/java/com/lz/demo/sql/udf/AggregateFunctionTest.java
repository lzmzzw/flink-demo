package com.lz.demo.sql.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 在创建表的ddl中直接定义时间属性
        String inputDDL = "CREATE TABLE EventTable(" +
                " `user` STRING," +
                " url STRING," +
                " `timestamp` BIGINT," +
                " et as to_timestamp(from_unixTime(`timestamp` / 1000))," +
                " watermark for et as et - interval '1' second" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(inputDDL);

        // 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvgFunction.class);

        // 使用自定义函数进行查询
        Table result = tableEnv.sqlQuery("select user, WeightedAvg(`timestamp`, 1) from EventTable group by user");
        tableEnv.toChangelogStream(result).print();

        env.execute();
    }

    // 自定义实现aggregate function，计算加权平均值
    public static class WeightedAvgFunction extends AggregateFunction<Long, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Long getValue(Tuple2<Long, Long> accumulator) {
            return accumulator.f0 == 0 ? null : accumulator.f0 / accumulator.f1;
        }

        // 定义更新累加器方法
        public void accumulate(Tuple2<Long, Long> accumulator, Long value, Long weight) {
            accumulator.f0 += value * weight;
            accumulator.f1 += weight;
        }
    }
}
