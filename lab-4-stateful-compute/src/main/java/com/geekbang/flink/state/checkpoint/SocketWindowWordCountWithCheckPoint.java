package com.geekbang.flink.state.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Window CheckPoint配置
 */
public class SocketWindowWordCountWithCheckPoint {

    public static void main(String[] args) throws Exception {
        // 获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9000--java");
            port = 9000;
        }

        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);

        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置statebackend
        env.getCheckpointConfig()
                .setCheckpointStorage("file://" + System.getProperty("user.dir")
                        + "/flink_workspace/flink-chk/SocketWindowWordCountWithCheckPoint");

        String hostname = "localhost";
        String delimiter = "\n";
        // 连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        System.out.println("==== Flink Job Started, listening on port " + port + " ====");

        DataStream<WordWithCount> windowCounts = text
                .flatMap((FlatMapFunction<String, WordWithCount>) (value, out) -> {
                    System.out.println("📥 Received line: " + value);
                    String[] splits = value.split("\\s+");
                    for (String word : splits) {
                        if (!word.isEmpty()) {
                            WordWithCount wc = new WordWithCount(word, 1L);
                            System.out.println("   -> Emit: " + wc);
                            out.collect(wc);
                        }
                    }
                })
                .returns(TypeInformation.of(WordWithCount.class)) // 必须加 returns
                .keyBy(wc -> wc.word)
                .window(org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows.of(
                        Time.seconds(4),
                        Time.seconds(1)))
                .sum("count");
                // .keyBy(wc -> wc.word) // ✅ 推荐写法，避免 POJO 限制
                // .timeWindow(Time.seconds(2), Time.seconds(1)) // 滑动窗口
                // .sum("count"); // 聚合 count

        // 把数据打印到控制台并且设置并行度
        windowCounts.map(wc -> {
            System.out.println("✅ Window Result: " + wc);
            return wc;
        }).print().setParallelism(1);

        env.execute("Socket window count with checkpoint");
    }

    public static class WordWithCount implements java.io.Serializable {
        public String word;
        public long count;

        // 推荐加上无参构造，虽然这里用 KeySelector 就没强制要求
        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
