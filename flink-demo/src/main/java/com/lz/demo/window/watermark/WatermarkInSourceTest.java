package com.lz.demo.window.watermark;

import com.lz.demo.entity.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WatermarkInSourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }

    // 泛型是数据源中的类型
    public static class ClickSourceWithWatermark implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                // 毫秒时间戳
                long currTs = Calendar.getInstance().getTimeInMillis();

                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);

                // 使用使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.timestamp);

                // 发送水位线
                sourceContext.emitWatermark(new Watermark(event.timestamp - 1L));
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
