package com.geekbang.flink.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

public class KafkaTableConnector {
    public static void main(String[] args) throws Exception {
        // ✅ 1. 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // ✅ 2. 注册 Kafka Source
        tableEnv.executeSql("""
            CREATE TABLE order_table (
              transactionId STRING,
              orderTime TIMESTAMP(3),
              orderItems ROW<orderItemId STRING, price DOUBLE>
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'order-input',
              'properties.bootstrap.servers' = 'localhost:9092',
              'properties.group.id' = 'flink-group',
              'scan.startup.mode' = 'earliest-offset',
              'format' = 'json',
              'json.ignore-parse-errors' = 'true'
            )
        """);

        // ✅ 3. 注册 Sink（写到本地 CSV 文件）
        tableEnv.executeSql("""
            CREATE TABLE result_table (
              transactionId STRING,
              orderTime TIMESTAMP(3),
              price DOUBLE
            ) WITH (
              'connector' = 'filesystem',
              'path' = '/Users/liuyingjie/flink_workspace/sink-result/order_result',
              'format' = 'csv'
            )
        """);

        // ✅ 4. SQL 逻辑
        tableEnv.executeSql("""
            INSERT INTO result_table
            SELECT transactionId, orderTime, orderItems.price
            FROM order_table
        """);
    }
}
