package com.lz.demo.source;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取
        DataStreamSource<String> steam1 = env.readTextFile("input/clicks.txt");

        // 2.从集合中读取
        List<Integer> numbers = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5));
        DataStreamSource<Integer> steam2 = env.fromCollection(numbers);

        // 3.从元素读取
        DataStreamSource<Event> steam3 = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L)
        );

        // 4.从socket流读取
        DataStreamSource<String> steam4 = env.socketTextStream("192.168.56.3", 9999);

        // 5.从kafka读取
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per unit
                Time.of(5, TimeUnit.MINUTES), // time interval for measuring failure rate
                Time.of(60, TimeUnit.SECONDS)) // delay
        );
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.56.4:9092,192.168.56.5:9092,192.168.56.6:9092");
        properties.setProperty("group.id", "demo");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> steam5 = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties));

        // 6.自定义source
        DataStreamSource<Event> steam6 = env.addSource(new ClickSource());

        // 6.自定义并行source
        DataStreamSource<Event> steam7 = env.addSource(new ParallelClickSource()).setParallelism(4);

        steam1.print("1");
        steam2.print("2");
        steam3.print("3");
        steam4.print("4");
        steam5.print("5");
        steam6.print("6");
        steam7.print("7");

        env.execute();
    }
}
