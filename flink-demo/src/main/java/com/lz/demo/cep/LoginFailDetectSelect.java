package com.lz.demo.cep;

import com.lz.demo.entity.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LoginFailDetectSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 8000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, l) -> element.timestamp)
                )
                .keyBy(r -> r.userId);

        // 1 定义 Pattern，连续的三个登录失败事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                // 以第一个登录失败事件开始
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                // 第二个登录失败事件
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                // 第三个登录失败事件
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) {
                        return loginEvent.eventType.equals("fail");
                    }
                });

        // 2 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // 3 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream.select(
                (PatternSelectFunction<LoginEvent, String>) map -> {
                    LoginEvent first = map.get("first").get(0);
                    LoginEvent second = map.get("second").get(0);
                    LoginEvent third = map.get("third").get(0);
                    return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
                })
                .print("warning");
        env.execute();
    }
}
