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


public class TumbleWindow {
    public static void main(String[] args) throws Exception {

        String outputPath = "/Users/liuyingjie/flink_workspace/sink-result/tumble-window";

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        sEnv.enableCheckpointing(4000);
        sEnv.getConfig().setAutoWatermarkInterval(1000);
        sEnv.getConfig().disableGenericTypes(); // 放在创建 env 后

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);

        // ✅ Replace deprecated registerTableSource with TableDescriptor + datagen connector
        tEnv.createTemporaryTable(
                "table1",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder()
                                .column("key", DataTypes.INT())
                                .column("price", DataTypes.DOUBLE())
                                .columnByExpression("rowtime", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '0' SECOND")
                                .build())
                        .option("rows-per-second", "10")
                        .option("fields.key.kind", "sequence")
                        .option("fields.key.start", "1")
                        .option("fields.key.end", "100")
                        .option("fields.price.min", "1")
                        .option("fields.price.max", "100")
                        .build()
        );

        int overWindowSizeSeconds = 10;

        String overQuery = String.format(
                "SELECT " +
                        "  key, " +
                        "  rowtime, " +
                        "  COUNT(*) OVER (PARTITION BY key ORDER BY rowtime RANGE BETWEEN INTERVAL '%d' SECOND PRECEDING AND CURRENT ROW) AS cnt " +
                        "FROM table1",
                overWindowSizeSeconds);

        Table result = tEnv.sqlQuery(overQuery);

        DataStream<Row> resultStream = tEnv.toDataStream(result);

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), (Encoder<Row>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element.toString());
                })
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        resultStream.print();

        DataStream<Row> mapped = resultStream
        .map(new ConfigurableKillMapper())
        .returns(resultStream.getType())      // 保留 RowTypeInfo
        .name("ConfigurableKillMapper");
        mapped.addSink(sink).setParallelism(1).name("FileSink");

        sEnv.execute("Tumble Window Example");
    }
}

/**
         * 可配置“在第 N 条记录”触发一次性失败（仅首次 attempt）。
         */
class ConfigurableKillMapper
                implements MapFunction<Row, Row>, CheckpointedFunction {

        private static final Logger LOG = LoggerFactory.getLogger(ConfigurableKillMapper.class);

        private final int killAtRecord;       // 第几条触发
        private final boolean enabled;

        // checkpointed
        private long processed = 0L;

        // non-checkpointed（attempt 内计数）
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
                // 只有第一次 attempt 满足：processed == attemptProcessed （因为恢复后 attemptProcessed 清零）
                if (processed == attemptProcessed &&
                        attemptProcessed + 1 == killAtRecord) {

                        LOG.warn("ConfigurableKillMapper: intentionally failing at record {} (first attempt)",
                                killAtRecord);
                        throw new RuntimeException("Intentional failure at record " + killAtRecord);
                }
                }
                processed++;
                attemptProcessed++;
                return value;
        }

        /* ---------- CheckpointedFunction 实现 ---------- */

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
                // attemptProcessed 不恢复，保持 0 用来判定“是否首次 attempt”
        }
}
