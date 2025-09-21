#!/bin/bash

# 项目目录
APP_DIR=$(cd "$(dirname "$0")"; pwd)

# Maven 构建（跳过测试）
echo "[INFO] Building project..."
mvn clean package -DskipTests

# 构建 classpath
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt -q
CLASSPATH="target/classes:$(cat cp.txt)"

# 启动主类
MAIN_CLASS="com.geekbang.flink.project.PVAndUVExample"

# 程序参数（按需修改）
APP_ARGS="--index test_index"

# JVM 参数：解决 Java 17 反射访问限制
# JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED"/
JAVA_OPTS="\
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.io=ALL-UNNAMED"


echo "[INFO] Running $MAIN_CLASS with Java 17 compatible settings..."
java $JAVA_OPTS -cp "$CLASSPATH" $MAIN_CLASS $APP_ARGS