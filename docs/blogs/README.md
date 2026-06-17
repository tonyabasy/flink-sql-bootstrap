# Flink 实时数仓开发实战 · 系列文章

基于 [Flink SQL Bootstrap](https://github.com/tonyabasy/flink-sql-bootstrap) 项目，从原理、实战、最佳实践三个维度，系统阐述 Flink SQL 的核心能力。

## 系列目录

| 序号 | 标题 | 概览 |
|------|------|------|
| 1 | [Flink 实时数仓开发实战：像 Hive 那样用 Flink SQL](./01-hive-like-flink-sql.md) | 像 `hive -f` 一样用 `flink run` 直接提交多语句 SQL 文件，兼容多种运行环境与部署模式 |
| 2 | [Flink 实时数仓开发实战：CI/CD 之道 — 从语法校验到自动化部署](./02-cicd-pipeline.md) | 为 Flink SQL 作业构建覆盖语法校验、多版本兼容矩阵到自动部署的 CI/CD 流水线 |
| 3 | [Flink 实时数仓开发实战：Catalog 快照 — 告别 DDL 地狱](./03-catalog-snapshot.md) | 用 Catalog 快照替代重复 DDL 定义，实现元数据多协议加载与 UDF 统一管理 |
| 4 | [Flink 实时数仓开发实战：细粒度资源配置原理与实践](./04-fine-grained-resource.md) | 通过算子级资源注入、SlotSharingGroup 和确定性 UID 实现细粒度资源调优 |
| 5 | [Flink 实时数仓开发实战：元数据管理与血缘采集](./05-metadata-lineage.md) | 介绍 Flink SQL 作业血缘关系的采集方式与管理机制（编写中） |
| 6 | [Flink 实时数仓开发实战：AI 驱动的 SQL 开发新范式](./06-ai-powered-sql.md) | 探索 AI 在实时数仓开发中的应用，提升 SQL 开发效率与质量（编写中） |

## 阅读建议

- **新用户想了解项目价值**：按顺序阅读第 1~2 篇
- **元数据管理**：直接跳第 3 篇
- **性能调优**：直接跳第 4 篇

## 项目仓库

[https://github.com/tonyabasy/flink-sql-bootstrap](https://github.com/tonyabasy/flink-sql-bootstrap)
