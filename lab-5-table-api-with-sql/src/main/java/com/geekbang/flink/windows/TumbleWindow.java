package com.geekbang.flink.windows;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按事件时间的 10 秒 Tumble 窗口，统计每个 key 的平均价格。
 * 支持乱序事件（Watermark 延迟 3 秒），持续运行。
 */
public class TumbleWindow {
    public static void main(String[] args) throws Exception {

        String outputPath = "/Users/liuyingjie/flink_workspace/sink-result/tumble-window";

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        sEnv.enableCheckpointing(4000);
        sEnv.getConfig().setAutoWatermarkInterval(1000);
        sEnv.getConfig().disableGenericTypes();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        // ✅ 模拟数据源 (datagen)
        // Watermark 延迟 3 秒：表示容忍乱序到达
        tEnv.createTemporaryTable(
                "table1",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder()
                                .column("key", DataTypes.INT())
                                .column("price", DataTypes.DOUBLE())
                                // 模拟事件时间列
                                .columnByExpression("rowtime", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '3' SECOND") // 允许乱序 3 秒
                                .build())
                        .option("rows-per-second", "50")
                        .option("fields.key.kind", "random")
                        .option("fields.key.min", "1")
                        .option("fields.key.max", "5")
                        .option("fields.price.min", "10")
                        .option("fields.price.max", "100")
                        .option("number-of-rows", "1000") 
                        .build()
        );

        // ✅ 按事件时间滚动窗口 (10 秒)
        String tumbleQuery =
                "SELECT " +
                        "  key, " +
                        "  window_start, " +
                        "  window_end, " +
                        "  AVG(price) AS avg_price, " +
                        "  COUNT(*) AS cnt " +
                        "FROM TABLE( " +
                        "  TUMBLE(TABLE table1, DESCRIPTOR(rowtime), INTERVAL '10' SECOND)" +
                        ") " +
                        "GROUP BY key, window_start, window_end";

        Table result = tEnv.sqlQuery(tumbleQuery);

        // ✅ 输出结果
        DataStream<Row> resultStream = tEnv.toDataStream(result);

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element.toString());
                })
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        resultStream.print();

        // ✅ 不再触发异常
        DataStream<Row> mapped = resultStream
                .map(new ConfigurableKillMapper(3, false))
                .returns(resultStream.getType())
                .name("ConfigurableKillMapper");

        mapped.addSink(sink).setParallelism(1).name("FileSink");

        sEnv.execute("Tumble Window with EventTime & Watermark Example");
    }
}

/**
 * 保留原 ConfigurableKillMapper，可选失败逻辑。
 * 当前默认禁用。
 */
class ConfigurableKillMapper implements MapFunction<Row, Row>, CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableKillMapper.class);

    private final int killAtRecord;
    private final boolean enabled;

    private long processed = 0L;
    private long attemptProcessed = 0L;
    private transient ListState<Long> checkpointedState;

    public ConfigurableKillMapper() {
        this(2, true);
    }

    public ConfigurableKillMapper(int killAtRecord, boolean enabled) {
        this.killAtRecord = killAtRecord;
        this.enabled = enabled;
    }

    @Override
    public Row map(Row value) {
        if (enabled) {
            if (processed == attemptProcessed && attemptProcessed + 1 == killAtRecord) {
                LOG.warn("ConfigurableKillMapper: intentionally failing at record {} (first attempt)", killAtRecord);
                throw new RuntimeException("Intentional failure at record " + killAtRecord);
            }
        }
        processed++;
        attemptProcessed++;
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(processed);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> desc =
                new ListStateDescriptor<>("killmapper-processed", Long.class);
        checkpointedState = context.getOperatorStateStore().getListState(desc);
        processed = 0L;
        for (Long v : checkpointedState.get()) {
            processed += v;
        }
    }
}
