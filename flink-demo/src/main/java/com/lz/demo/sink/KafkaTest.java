package com.lz.demo.sink;

import com.lz.demo.entity.Event;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从kafka读取
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

        DataStreamSource<String> steam = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties));

        // 2.用flink转换处理
        SingleOutputStreamOperator<String> result = steam.map(v -> {
            String[] fields = v.split(",");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
        });

        // 3.结果数据写入kafka
        result.addSink(new FlinkKafkaProducer<>("192.168.56.4:9092,192.168.56.5:9092,192.168.56.6:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}
