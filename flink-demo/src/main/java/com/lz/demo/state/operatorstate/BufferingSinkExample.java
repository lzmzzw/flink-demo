package com.lz.demo.state.operatorstate;

import com.lz.demo.entity.Event;
import com.lz.demo.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从源读取，乱序流watermark生成
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(5));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义阈值
        private final int threshold;
        private final List<Event> bufferedElements;
        // 定义算子状态
        private transient ListState<Event> checkpointState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) {
            // 缓存到列表
            bufferedElements.add(value);

            // 如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                for (Event element : bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("======================================== 输出完毕 =======================================");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 复制前清空状态
            checkpointState.clear();

            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Types.POJO(Event.class));

            checkpointState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
