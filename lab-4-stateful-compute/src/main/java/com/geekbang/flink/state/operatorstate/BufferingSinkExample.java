package com.geekbang.flink.state.operatorstate;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferingSinkExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // checkpoint
        env.enableCheckpointing(1000);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setMinPauseBetweenCheckpoints(2000);
        ck.setCheckpointTimeout(60000);
        ck.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/liuyingjie/flink_workspace/flink-chk");

        // 重启策略：失败后 3 次，每次间隔 5 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(5)));

        // 无限数据（演示用：1 到 Long.MAX_VALUE）
        DataStream<Tuple2<String, Integer>> source =
                env.fromSequence(1, Long.MAX_VALUE)
                   .map(v -> Tuple2.of("key", v.intValue()))
                   .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        // 添加一个“只失败一次”的 map 来触发恢复
        DataStream<Tuple2<String, Integer>> faulty =
                source.map(new FailingOnceMap(20_000)) // 处理到第 20000 条抛异常一次
                      .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        faulty.addSink(new BufferingSinkFunction(9))
              .name("buffering-sink")
              .uid("buffering-sink-uid"); // 生产建议显式 uid，利于升级/恢复匹配

        env.execute("Buffering Sink Operator State Demo (FailOnce)");
    }

    // 只在第一次达到阈值时抛异常，触发重启
    static class FailingOnceMap implements org.apache.flink.api.common.functions.MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private final int failAt;
        private static final AtomicInteger global = new AtomicInteger(0);
        private static volatile boolean failed = false;

        FailingOnceMap(int failAt) {
            this.failAt = failAt;
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
            int c = global.incrementAndGet();
            if (!failed && c >= failAt) {
                failed = true;
                System.out.println(">>> Artificial failure at element count=" + c);
                throw new RuntimeException("Fail once to test restore");
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return value;
        }
    }
}

class BufferingSinkFunction implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;
    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSinkFunction(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        bufferedElements.add(value);
        if (bufferedElements.size() >= threshold) {
            flushBuffer("THRESHOLD_REACHED");
        }
    }

    private void flushBuffer(String reason) {
        if (bufferedElements.isEmpty()) return;
        // System.out.printf("[%s] %s flush %d elements (reason=%s) %s%n",
        //         Instant.now(),
        //         Thread.currentThread().getName(),
        //         bufferedElements.size(),
        //         reason,
        //         bufferedElements);
        bufferedElements.clear();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> e : bufferedElements) {
            checkpointedState.add(e);
        }
        System.out.printf("[SNAPSHOT] chkId=%d subtask=%s buffered size=%d%n",
                context.getCheckpointId(),
                Thread.currentThread().getName(),
                bufferedElements.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>("buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> e : checkpointedState.get()) {
                bufferedElements.add(e);
            }
            System.out.printf("[RESTORED] %s recovered %d buffered: %s%n",
                    Thread.currentThread().getName(),
                    bufferedElements.size(),
                    bufferedElements);
        } else {
            System.out.printf("[FRESH] %s start new instance%n", Thread.currentThread().getName());
        }
    }
}