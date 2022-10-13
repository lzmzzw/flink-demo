package com.lz.demo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowTopNExample {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 在创建表的ddl中直接定义时间属性
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

        // 定义 Top N 的外层查询
        String topNQuery = "select * from" +
                " (select *, " +
                " row_number() over (order by cnt desc) as row_num" +
                " from (select user, count(url) as cnt from EventTable group by user))" +
                " where row_num <= 2";

        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery = "select window_start, window_end, user, count(url) as cnt" +
                " from table(" +
                " tumble(table EventTable, descriptor(et), interval '5' second))" +
                " group by window_start, window_end, user";

        // 定义 Window Top N 的外层查询
        String windowTopNQuery = "select * from" +
                " (select *, " +
                " row_number() over (partition by window_start, window_end order by cnt desc) as row_num" +
                " from (" + subQuery + "))" +
                " where row_num <= 2";

        // 执行 SQL 得到结果表
        Table topNResult = tableEnv.sqlQuery(topNQuery);
        Table windowTopNResult = tableEnv.sqlQuery(windowTopNQuery);
        tableEnv.toChangelogStream(topNResult).print("topNResult");
        tableEnv.toDataStream(windowTopNResult).print("windowTopNResult");

        env.execute();
    }
}
