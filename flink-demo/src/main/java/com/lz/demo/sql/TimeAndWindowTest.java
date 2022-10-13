package com.lz.demo.sql;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
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
        Table table1 = tableEnv.sqlQuery("select * from EventTable");

        // 1.2 在流转换成table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, recordTimestamp) -> event.timestamp));
        Table table2 = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp"),
                $("et").rowtime());
        tableEnv.createTemporaryView("ClickTable", table2);

        table1.printSchema();
        table2.printSchema();

        // 2.1 滚动窗口聚合
        Table tumbleWindowResult = tableEnv.sqlQuery("select user, window_end as entT, count(url) as cnt" +
                " from table(" +
                " tumble(table EventTable, descriptor(et), interval '10' second)" +
                ")" +
                " group by user, window_start, window_end");

        // 2.2 滑动窗口聚合
        Table hopWindowResult = tableEnv.sqlQuery("select user, window_end as entT, count(url) as cnt" +
                " from table(" +
                " hop(table EventTable, descriptor(et), interval '5' second, interval '10' second)" +
                ")" +
                " group by user, window_start, window_end");

        // 2.3 累积窗口聚合
        Table cumulateWindowResult = tableEnv.sqlQuery("select user, window_end as entT, count(url) as cnt" +
                " from table(" +
                " cumulate(table EventTable, descriptor(et), interval '5' second, interval '10' second)" +
                ")" +
                " group by user, window_start, window_end");

        // 2.4 开窗函数聚合
        Table overWindowResult = tableEnv.sqlQuery("select user," +
                " avg(`timestamp`) over(partition by user order by et rows between 3 preceding and current row) as avg_ts" +
                " from EventTable");

        tableEnv.toDataStream(tumbleWindowResult).print("tumbleWindowResult");
        tableEnv.toDataStream(hopWindowResult).print("hopWindowResult");
        tableEnv.toDataStream(cumulateWindowResult).print("cumulateWindowResult");
        tableEnv.toDataStream(overWindowResult).print("overWindowResult");

        env.execute();
    }
}
