<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

# Flink SQL Bootstrap

一个生产级的 Flink SQL Application 增强模板，提供自定义 Catalog 快照、Multi-Statement SQL Script 部署和细粒度资源调优三大能力。

## 解决什么问题

原生 Flink SQL 三个痛点：

1. **Catalog 不可复用** — DDL 与作业耦合，每次提交都要重建表，无法对接外部元数据
2. **不支持 Multi-Statement SQL Script** — 只能逐条执行，无法将多条 SQL 作为整体编排、校验和提交
3. **不支持细粒度资源调优** — 细粒度资源调优为资源优化提供了巨大空间，但可惜 Flink SQL 官方不支持

Flink SQL Bootstrap 正是为解决这三个问题而生。

## 能力边界

详见 [docs/CAPABILITIES_CN.md](docs/CAPABILITIES_CN.md)。

## 快速开始

### 环境要求

| 依赖 | 版本 |
|------|------|
| Java | 11+ |
| Flink | 1.20.x / 2.2.0 |


### 启动方式

```bash
# 编译
mvn package -DskipTests

# 提交（使用所有选项）
$FLINK_HOME/bin/flink run \
    --target local \
    -Dpipeline.name="My First SQL Job" \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count.sql \
    --resource-file classpath:example-resource.json \
    --catalog-file classpath:example-catalog.json \
    --dependency classpath:example-udf-reverse.jar \
    --dependency classpath:example-udf-substring.jar

# 所有文件参数支持以下协议：
#   classpath:  - 从应用 JAR 资源中读取
#   http(s)://  - 从远程 HTTP 服务器获取
#   /path/to    - 从本地文件系统读取
#   hdfs://     - 通过 Flink FileSystem 从 HDFS 或其他 DFS 读取
#   s3://       - 通过 Flink FileSystem 从 S3 读取
```

## 如何贡献

待补充。