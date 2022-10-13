package com.lz.demo.sql.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableAggregateFunctionTest {
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

        // 注册自定义表聚合函数
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        String subQuery = "select window_start, window_end, user, count(url) as cnt" +
                " from table(" +
                " tumble(table EventTable, descriptor(et), interval '5' second))" +
                " group by window_start, window_end, user";
        Table aggTable = tableEnv.sqlQuery(subQuery);

        // 使用自定义函数进行查询
        Table result = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        tableEnv.toChangelogStream(result).print();

        env.execute();
    }

    // 定义一个累加器
    public static class Top2Accumulator {
        public Long max = Long.MIN_VALUE;
        public Long secondMax = Long.MIN_VALUE;
    }

    // 自定义实现table aggregate function
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            return new Top2Accumulator();
        }

        // 定义更新累加器方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.max) {
                accumulator.secondMax = accumulator.max;
                accumulator.max = value;
            } else if (value > accumulator.secondMax) {
                accumulator.secondMax = value;
            }
        }

        // 输出结果
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> collector) {
            if (accumulator.max != Long.MIN_VALUE) {
                collector.collect(Tuple2.of(accumulator.max, 1));
            }
            if (accumulator.secondMax != Long.MIN_VALUE) {
                collector.collect(Tuple2.of(accumulator.secondMax, 2));
            }
        }
    }
}
