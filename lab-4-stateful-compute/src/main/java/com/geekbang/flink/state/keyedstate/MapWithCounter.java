package com.geekbang.flink.state.keyedstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Arrays;

/**
 * 演示 Keyed State (ValueState) 使用：
 * 对相同 key 的字符串 value 累加其长度，输出“当前累计长度”。
 *
 * 运行：
 *  1) 直接运行（使用内置示例数据）
 *  2) Socket 模式：
 *     启动 nc:  nc -lk 9999
 *     发送:    a,hello
 *              a,flink
 *              b,xyz
 *     运行 main 并带参： --host localhost --port 9999
 */
public class MapWithCounter {

    public static void main(String[] args) throws Exception {

        // 简单参数解析
        String host = null;
        Integer port = null;
        for (int i = 0; i < args.length; i++) {
            if ("--host".equals(args[i]) && i + 1 < args.length) {
                host = args[i + 1];
            } else if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, String>> source;

        if (!StringUtils.isNullOrWhitespaceOnly(host) && port != null) {
            // Socket 文本输入，每行格式: key,value
            source = env
                    .socketTextStream(host, port)
                    .filter(line -> !StringUtils.isNullOrWhitespaceOnly(line) && line.contains(","))
                    .map(line -> {
                        String[] parts = line.split(",", 2);
                        return Tuple2.of(parts[0].trim(), parts[1].trim());
                    })
                    .returns(Types.TUPLE(Types.STRING, Types.STRING));
        } else {
            // 内置示例数据
            source = env.fromCollection(Arrays.asList(
                    Tuple2.of("a", "hello"),
                    Tuple2.of("b", "hi"),
                    Tuple2.of("a", "flink"),
                    Tuple2.of("b", "state"),
                    Tuple2.of("a", "rocks"),
                    Tuple2.of("b", "kv"),
                    Tuple2.of("a", "operator"),
                    Tuple2.of("b", "keyed")
            ));
        }

        // 必须 keyBy 后才能使用 keyed state
        DataStream<Tuple2<String, Long>> aggregatedLengths =
                source
                        .keyBy(t -> t.f0)  // 以 key (f0) 分区
                        .map(new MapWithCounterFunction())
                        .name("map-with-counter");

        aggregatedLengths.print().name("print");

        env.execute("MapWithCounter (Keyed ValueState) Demo");
    }
}

class MapWithCounterFunction extends RichMapFunction<
        Tuple2<String, String>,
        Tuple2<String, Long>> {

    private transient ValueState<Long> totalLengthByKey;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<>("sum-of-length", LongSerializer.INSTANCE);
        totalLengthByKey = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
        Long current = totalLengthByKey.value();
        if (current == null) {
            current = 0L;
        }
        long newTotal = current + value.f1.length();
        totalLengthByKey.update(newTotal);
        return Tuple2.of(value.f0, newTotal); // 把 key 带回去
    }
}