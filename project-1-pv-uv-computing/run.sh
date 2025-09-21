#!/bin/bash

# 构建项目
mvn clean package -DskipTests

# 启动程序（classpath 包括依赖和类文件）
java -cp "target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=cp.txt && cat cp.txt)" \
  --add-opens java.base/java.util=ALL-UNNAMED \
  com.geekbang.flink.project.PVAndUVExample \
  --index test_index