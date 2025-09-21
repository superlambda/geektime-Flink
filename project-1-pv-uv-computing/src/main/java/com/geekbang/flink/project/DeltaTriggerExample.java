package com.geekbang.flink.project;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 模拟传感器温度数据，使用 DeltaTrigger 检测温度波动
 */
public class DeltaTriggerExample {

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 模拟传感器数据流
        SingleOutputStreamOperator<SensorReading> stream = env
            .addSource(new SensorSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((event, ts) -> event.timestamp)
            );

        // 3. 使用 DeltaTrigger 检测温度变化
        stream
            .keyBy(sensor -> sensor.id)
            .window(GlobalWindows.create()) // 使用全局窗口 + 触发器
            .trigger(
                DeltaTrigger.of(
                2.0, // 温度变化阈值 2°C
                new DeltaFunction<SensorReading>() {
                    @Override
                    public double getDelta(SensorReading last, SensorReading next) {
                        return Math.abs(last.temperature - next.temperature);
                    }
                },
                // TypeInformation.of(SensorReading.class)
                TypeInformation.of(SensorReading.class).createSerializer(new ExecutionConfig())
            )
            )
            .process(new AlertFunction())
            .print();

        env.execute("DeltaTrigger 温度波动监控");
    }

    // 模拟传感器读取
    public static class SensorReading {
        public String id;
        public long timestamp;
        public double temperature;

        public SensorReading() {}

        public SensorReading(String id, long timestamp, double temperature) {
            this.id = id;
            this.timestamp = timestamp;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return String.format("Sensor(%s): %.2f°C @ %d", id, temperature, timestamp);
        }
    }

    // 自定义数据源：每秒生成一次温度
    public static class SensorSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<SensorReading> {
        private volatile boolean running = true;
        private Random rand = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            double currentTemp = 25.0;
            while (running) {
                currentTemp += rand.nextGaussian(); // 随机波动
                ctx.collect(new SensorReading("sensor_1", System.currentTimeMillis(), currentTemp));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 触发后执行的逻辑（可以报警/聚合）
    public static class AlertFunction extends ProcessWindowFunction<SensorReading, String, String, GlobalWindow> {
        @Override
        public void process(String key, Context context, Iterable<SensorReading> readings, Collector<String> out) {
            SensorReading latest = readings.iterator().next(); // DeltaTrigger 每次只收一个新值
            out.collect("🚨 温度变化报警：" + latest);
        }
    }
}

