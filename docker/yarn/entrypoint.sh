#!/bin/bash
set -e

# 导出 Hadoop 类路径（Flink YARN 部署需要）
export HADOOP_CLASSPATH=$(hadoop classpath)

# 首次启动格式化 HDFS
if [ ! -d "/var/hadoop/dfs/name/current" ]; then
  echo "Formatting HDFS NameNode ..."
  hdfs namenode -format -force -nonInteractive
fi

# 启动 HDFS
echo "Starting HDFS NameNode ..."
hdfs --daemon start namenode
echo "Starting HDFS DataNode ..."
hdfs --daemon start datanode

# 启动 YARN
echo "Starting YARN ResourceManager ..."
yarn --daemon start resourcemanager
echo "Starting YARN NodeManager ..."
yarn --daemon start nodemanager

# 为 Flink 兼容性测试创建 HDFS 目录
hdfs dfs -mkdir -p /flink-dist

echo "HDFS + YARN ready."

# 保持容器运行
tail -f /dev/null
