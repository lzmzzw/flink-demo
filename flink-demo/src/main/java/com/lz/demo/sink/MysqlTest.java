package com.lz.demo.sink;

import com.lz.demo.entity.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=10", 4000L),
                new Event("Tom", "/prod?id=10", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L),
                new Event("Jerry", "/home", 8000L)
        );

        stream.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (?, ?)",
                ((statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.56.3:3306/demo")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();
    }
}
