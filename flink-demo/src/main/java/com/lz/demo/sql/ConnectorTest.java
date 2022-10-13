package com.lz.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ConnectorTest {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 创建输入表
        String inputDDL = "CREATE TABLE EventTable(" +
                " `user` STRING," +
                " url STRING," +
                " `timestamp` BIGINT" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(inputDDL);

        // 1.2 创建输出表
        String outputDDL = "CREATE TABLE OutputTable(" +
                " `user` STRING," +
                " url STRING" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output/clicks.json'," +
                " 'format' = 'json'" +
                ")";
        tableEnv.executeSql(outputDDL);

        // 1.3 创建控制台输出表
        String printDDL = "CREATE TABLE PrintTable(" +
                " `user` STRING," +
                " `count` BIGINT" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";
        tableEnv.executeSql(printDDL);

        // sql查询
        Table result = tableEnv.sqlQuery("select user, url from EventTable");
        Table aggResult = tableEnv.sqlQuery("select user, count(1) as `count` from EventTable group by user");

        // 2.1 输出数据，以下两种方式等价
        result.executeInsert("OutputTable");
        // tableEnv.executeSql("insert into OutputTable select user, url from EventTable");

        // 2.2 输出到控制台
        aggResult.executeInsert("PrintTable");

        // executeSql()/executeInsert()方法已经执行了sql语句，没有后续流操作的话，不需要再使用env.execute()方法

        // 2.3 转化为流后输出到控制台
        tableEnv.toDataStream(result).print("result");
        tableEnv.toChangelogStream(aggResult).print("aggResult");

        env.execute();
    }
}
