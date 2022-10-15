package com.lz.demo.processfunction;

import com.lz.demo.entity.Event;
import com.lz.demo.entity.UrlCount;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KeyedProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置水位线的时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );
        stream.print();

        // 1.按url分组统计每个窗口内的访问量
        SingleOutputStreamOperator<UrlCount> urlCountStream = stream
                .keyBy(v -> v.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new UrlCountAggregate(), new UrlCountResult());

        // 2.获取每个窗口内的top2访问
        urlCountStream
                .keyBy(v -> v.windowEnd)
                .process(new TopNProcessResult(2))
                .print();


        env.execute();
    }

    /**
     * url访问次数
     */
    public static class UrlCountAggregate implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

    /**
     * 包装窗口信息输出
     */
    public static class UrlCountResult extends ProcessWindowFunction<Long, UrlCount, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Long, UrlCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlCount> collector) throws Exception {
            // 窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(new UrlCount(key, iterable.iterator().next(), start, end));
        }
    }

    /**
     * 实现自定义KeyedProcessFunction
     */
    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCount, String> {
        private final Integer n;

        // 定义列表状态
        private ListState<UrlCount> urlCountListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        // 在运行时环境中获取状态
        @Override
        public void open(Configuration parameters) {
            urlCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("url-count-list", Types.POJO(UrlCount.class))
            );
        }

        @Override
        public void processElement(UrlCount urlCount, KeyedProcessFunction<Long, UrlCount, String>.Context context, Collector<String> collector) throws Exception {
            // 将数据保存到状态中
            urlCountListState.add(urlCount);

            // 注册 windowEnd + 1ms 的定时器
            context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlCount> urlCountList = new ArrayList<>();
            urlCountListState.get().forEach(urlCountList::add);

            // 排序
            urlCountList.sort((o1, o2) -> o2.count.intValue() - o1.count.intValue());

            // 打印输出
            StringBuilder result = new StringBuilder();
            result.append("窗口结束时间：").append(new Timestamp(ctx.getCurrentKey())).append("\n");

            for (int i = 0; i < n; i++) {
                UrlCount urlCount = urlCountList.get(i);
                String info = "No. " + (i + 1) + " url " + urlCount.url + " 访问量： " + urlCount.count + "\n";
                result.append(info);
            }

            out.collect(result.toString());
        }
    }
}
