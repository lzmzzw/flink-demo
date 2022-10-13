package com.lz.demo.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class ScalarFunctionTest {
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

        // 注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFunction.class);

        // 使用自定义函数进行查询
        Table result = tableEnv.sqlQuery("select user, MyHash(user) from EventTable");
        tableEnv.toDataStream(result).print();

        env.execute();
    }

    // 自定义实现scalar function
    public static class MyHashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
