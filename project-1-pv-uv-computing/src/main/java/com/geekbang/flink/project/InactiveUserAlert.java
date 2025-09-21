package com.geekbang.flink.project;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class InactiveUserAlert {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 输入流模拟
        env
            .fromElements(
                new UserEvent("user1", 1000L),
                new UserEvent("user1", 3000L),
                new UserEvent("user2", 4000L),
                // user1 超过 5 秒没有上报 → 应触发定时器
                new UserEvent("user2", 10000L)
            )
            .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forMonotonousTimestamps()
                .withTimestampAssigner((event, ts) -> event.timestamp))
            .keyBy(event -> event.userId)
            .process(new UserInactivityDetector())
            .print();

        env.execute();
    }

    // 用户事件类
    public static class UserEvent {
        public String userId;
        public long timestamp;

        public UserEvent() {}
        public UserEvent(String userId, long timestamp) {
            this.userId = userId;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return userId + "@" + timestamp;
        }
    }

    // 处理函数：每次事件都会设置一个“5 秒后触发”的定时器
    public static class UserInactivityDetector extends KeyedProcessFunction<String, UserEvent, String> {

        private ValueState<Long> timerState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer-state", Long.class)
            );
        }

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<String> out) throws Exception {
            // 清除旧定时器
            Long oldTimer = timerState.value();
            if (oldTimer != null) {
                ctx.timerService().deleteEventTimeTimer(oldTimer);
            }

            // 注册新的定时器（当前事件时间 + 5 秒）
            long newTimer = event.timestamp + 5000L;
            ctx.timerService().registerEventTimeTimer(newTimer);

            // 保存定时器
            timerState.update(newTimer);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 当定时器触发，说明 5 秒内没有收到新的事件
            String userId = ctx.getCurrentKey();
            out.collect("⚠️ 用户 " + userId + " 在 " + timestamp + " 之前无活动，触发超时告警！");
            // 清除状态
            timerState.clear();
        }
    }
}
