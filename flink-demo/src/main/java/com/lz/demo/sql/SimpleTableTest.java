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

public class SimpleTableTest {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 用执行 SQL 的方式提取数据
        Table visitTable1 = tableEnv.sqlQuery("select url, user from " + eventTable);

        // 用 Table API 方式提取数据
        Table visitTable2 = eventTable.select($("url"), $("user"));

        // 创建虚拟表
        tableEnv.createTemporaryView("tempTable", eventTable);
        Table visitTable3 = tableEnv.sqlQuery("select url, user from tempTable");

        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable1).print("1");
        tableEnv.toDataStream(visitTable2).print("2");
        tableEnv.toDataStream(visitTable3).print("3");

        // 执行程序
        env.execute();
    }
}
