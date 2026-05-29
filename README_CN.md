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
| Flink | 2.2.0 |


### 启动方式

```bash
# 编译
mvn package -DskipTests

# 提交
flink run target/flink-sql-bootstrap.jar \
    --script-file /path/to/job.sql \
    --catalog-file /path/to/catalog.json \
    --resource-file /path/to/resource.json \
    --deps /path/to/udf.jar
```

## 如何贡献

待补充。