<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

<h1 align="center">Flink SQL Bootstrap</h1>

<p align="center">
  一个生产级的 Flink SQL 作业增强启动器 —— 自定义 Catalog 快照、Multi-Statement SQL Script 部署、算子级细粒度资源调优。
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/Java-11%2B-orange" alt="Java">
  <img src="https://img.shields.io/badge/Flink-1.20%2B-brightgreen" alt="Flink">
</p>

---

## 目录

- [为什么要用 Flink SQL Bootstrap](#为什么要用-flink-sql-bootstrap)
- [核心能力](#核心能力)
- [快速开始](#快速开始)
  - [环境要求](#环境要求)
  - [Step 1 — 运行 SQL 脚本](#step-1--运行-sql-脚本)
  - [Step 2 — 生成并注入资源配置](#step-2--生成并注入资源配置)
  - [Step 3 — 带 Catalog 快照部署](#step-3--带-catalog-快照部署)
- [运行模式](#运行模式)
- [配置参考](#配置参考)
  - [资源调优 JSON](#资源调优-json)
  - [Catalog 快照 JSON](#catalog-快照-json)
- [架构设计](#架构设计)
- [能力边界](#能力边界)
- [如何贡献](#如何贡献)
- [文档](#文档)
- [许可证](#许可证)

---

## 为什么要用 Flink SQL Bootstrap

原生 Flink SQL 在生产环境有三大痛点：

| 痛点 | 原生 Flink SQL | Flink SQL Bootstrap |
|-----------|------------------|---------------------|
| **Catalog 不可复用** | DDL 与作业强耦合，每次提交都要重建表。 | 通过 **Catalog 快照 JSON** 部署 —— 表、视图、UDF 预注册，运行时无需 DDL。 |
| **不支持多语句脚本** | 原生仅支持单条语句提交。 | 完整的 **Multi-Statement SQL Script** 支持 —— DDL 立即执行，DML 编译优化后作为单一 Pipeline 提交。 |
| **资源调优粒度粗** | 仅支持 TM/JM 级别的粗粒度配置（内存、slot）。 | **算子级资源注入** —— 为每个算子独立配置 CPU、堆内存、托管内存、并行度和链策略。 |

---

## 核心能力

1. **自定义 Catalog 快照** —— 将表、视图和 UDF 序列化为 JSON 文件。作业启动时 Catalog 已就绪，SQL 脚本中无需写 DDL。支持多协议快照加载，为**实时数仓元数据管理**提供遍历。
2. **Multi-Statement SQL Script** —— 在单个 `.sql` 文件中编写 `CREATE TABLE`、`SET`、`INSERT`、`CALL` 等语句。启动器自动切分、校验并编排执行。
3. **细粒度资源调优** —— 通过 `--init-resource` 生成资源模板，按算子调整并行度和资源，并在作业提交前注入 Flink DAG。
4. **多协议资源加载** —— 支持从 `classpath:`、`file://`、`http(s)://`、`hdfs://`、`s3://` 加载 SQL 脚本、Catalog 快照、细粒度资源配置。
5. **支持所有 Flink 部署模式** —— 支持 Standalone、Per-Job、Application、Session 模式，以及 Local、YARN、Kubernetes 等资源环境。
6. **SQL 语法校验与行级错误定位** —— 通过 `--validate` 在本地快速校验 SQL 语法，无需提交到 Flink 集群。错误信息精确到行号和列号，便于快速迭代。
7. **Flink 1.20+ 兼容** —— 适配 Flink 1.20 及以后版本，不修改 Flink 引擎或 Planner。（未来会持续适配更多版本）

---

## 快速开始

### 环境要求

| 依赖 | 版本 |
|------|------|
| Java | 11+ |
| Flink | 1.20+ |

### 编译

```bash
mvn package -DskipTests
```

### Step 1 — 运行 SQL 脚本

最简单的方式 —— 直接提交一条 SQL 脚本：

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count.sql
```

其中 `example-word-count.sql` 包含 **DDL + DML**，无需任何外部依赖：

```sql
-- datagen 自动生成句子 → 分词 → 分组计数 → print 输出
CREATE TEMPORARY TABLE source_table (
  sentence STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1'
);

CREATE TEMPORARY TABLE sink_table (
  word STRING,
  cnt BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO sink_table
SELECT word, COUNT(*) AS cnt
FROM source_table
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY word;
```

执行后将在控制台看到类似输出（实际值可能是随机字符串，因为 datagen 生成随机数据）：

```
Flink SQL> INSERT INTO sink_table
           SELECT word, COUNT(*) AS cnt ...
> +I[<random_hex_string>, 1]
> +I[<random_hex_string>, 1]
> +I[<random_hex_string>, 1]
```

### Step 2 — 生成并注入资源配置

从 SQL 脚本生成资源模板：

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count.sql \
    --init-resource
```

输出示例：

```json
{
  "version" : 1,
  "operators" : [ {
    "uid" : "1_source",
    "name" : "source_table[1]",
    "parallelism" : 1,
    "chainStrategy" : "HEAD"
  }, {
    "uid" : "2_correlate",
    "name" : "Correlate[2]",
    "parallelism" : 1,
    "chainStrategy" : "ALWAYS"
  }, {
    "uid" : "3_calc",
    "name" : "Calc[3]",
    "parallelism" : 1,
    "chainStrategy" : "ALWAYS"
  }, {
    "uid" : "5_group-aggregate",
    "name" : "GroupAggregate[5]",
    "parallelism" : -1,
    "chainStrategy" : "ALWAYS"
  }, {
    "uid" : "6_sink",
    "name" : "sink_table[6]",
    "parallelism" : -1,
    "chainStrategy" : "ALWAYS"
  } ]
}
```

生成的模板为每个算子分配了 `uid`、`name`、默认 `parallelism` 和 `chainStrategy`。修改这些字段可调整并行度和链策略，然后添加 `resource` 字段配置 CPU/内存（支持的字段见[资源调优 JSON](#资源调优-json)）。

将输出保存为 `resource.json`，修改参数后带资源配置提交：

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count.sql \
    --resource-file classpath:resource.json
```

### Step 3 — 带 Catalog 快照部署

通过 Catalog 快照预注册表和 UDF，使 SQL 脚本中 **不写任何 DDL**。需要配合专门的 SQL 脚本使用：

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count-advanced.sql \
    --catalog-file classpath:example-catalog.json \
    --resource-file classpath:example-resource.json \
    --dependency classpath:example-udf-reverse.jar \
    --dependency classpath:example-udf-substring.jar
```

其中 `example-word-count-advanced.sql` 只包含 DML（表和 UDF 已由 Catalog 快照预注册）：

```sql
INSERT INTO dws_word_count
SELECT my_reverse(my_substring(word, 0, 2)) AS word, COUNT(*) AS cnt
FROM ods_words
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY my_reverse(my_substring(word, 0, 2));
```

输出（注意 2 字符前缀 —— `my_reverse(my_substring(...))` UDF 已生效）：

```
Job has been submitted with JobID <job_id>
+I[6a, 1]
+I[00, 1]
+I[a3, 1]
+I[8f, 1]
```

---

## 运行模式

除了完整执行外，还提供三种干运行模式，适用于 CI/CD 和开发调试：

| 模式 | 参数 | 说明 |
|------|------|------|
| **语法校验** | `--validate` | 解析并校验 SQL 语法，不编译也不执行。 |
| **编译** | `--compile` | 解析、校验并编译 SQL 脚本，输出优化后的执行计划 JSON。 |
| **生成资源模板** | `--init-resource` | 翻译 SQL 计划并输出资源配置模板，供用户调优。 |

```bash
# 校验 SQL 语法
$FLINK_HOME/bin/flink run ... --script-file job.sql --validate

# 编译并查看执行计划
$FLINK_HOME/bin/flink run ... --script-file job.sql --compile

# 生成资源模板
$FLINK_HOME/bin/flink run ... --script-file job.sql --init-resource
```

---

## 配置参考

### 资源调优 JSON

描述每个算子的资源配置。每个算子通过 `uid`（优先）或 `name` 匹配。

```json
{
  "version": 1,
  "defaultParallelism": 2,
  "operators": [
    {
      "uid": "1_source",
      "name": "ods_words[1]",
      "parallelism": 1,
      "chainStrategy": "HEAD",
      "resource": {
        "profile": "small"
      }
    },
    {
      "uid": "5_group-aggregate",
      "name": "GroupAggregate[5]",
      "parallelism": 4,
      "chainStrategy": "ALWAYS",
      "resource": {
        "cpu": 1.0,
        "heap": "2048m",
        "managed": "256m"
      }
    }
  ]
}
```

**字段说明：**

| 字段 | 类型 | 说明 |
|-------|------|-------------|
| `version` | int | 配置格式版本，当前为 `1`。 |
| `defaultParallelism` | int | 全局默认并行度，`0` 表示不覆盖。 |
| `operators` | 数组 | 算子配置列表。 |
| `operators[].uid` | string | 稳定 UID，用于精确匹配。会覆盖 Flink 自动生成的 UID。 |
| `operators[].name` | string | 算子名称，作为 UID 匹配的兜底策略。 |
| `operators[].parallelism` | int | 算子并行度，`-1` 表示使用 Flink 默认。 |
| `operators[].chainStrategy` | string | `HEAD`、`ALWAYS` 或 `NEVER`，控制算子链合并策略。 |
| `operators[].resource.profile` | string | 预置规格：`small`、`normal`、`large`、`xlarge`。 |
| `operators[].resource.cpu` | double | CPU 核数（支持小数）。 |
| `operators[].resource.heap` | string | Task 堆内存，如 `"512 MB"`、`"2g"`。 |
| `operators[].resource.managed` | string | 托管内存，如 `"256m"`。 |

### Catalog 快照 JSON

描述一个自包含的 Catalog，包含表、视图和 UDF。

```json
{
  "version": 1,
  "snapshotId": "example-word-count",
  "catalogName": "platform",
  "databaseName": "default",
  "tables": [
    {
      "database": "default",
      "name": "ods_words",
      "columns": [
        { "name": "sentence", "type": "STRING", "nullable": true }
      ],
      "options": {
        "connector": "datagen",
        "rows-per-second": "1"
      }
    }
  ],
  "views": [],
  "udfs": [
    {
      "name": "my_reverse",
      "className": "examples.udf.MyReverseFunction",
      "functionLanguage": "JAVA",
      "jarRef": "example-udf-reverse.jar"
    }
  ]
}
```

---

## 架构设计

Flink SQL Bootstrap 是 Flink 官方 API 之上的一个轻量级编排层。**不修改** Flink 引擎、Planner 或 SQL 语义。

```
+------------------+     +---------------------+     +---------------------+
|   SQL 脚本       | --> |  StreamingScript    | --> |  Flink Table API    |
|  (DDL + DML)     |     |     Executor        |     |  (InternalPlan)     |
+------------------+     +---------------------+     +---------------------+
                                |                              |
                                v                              v
+------------------+     +---------------------+     +---------------------+
| Catalog 快照     | --> |  非 DML：立即执行   |     |  compilePlan()      |
|   (JSON)         |     |  DML：延迟批量处理  |     |  translatePlan()    |
+------------------+     +---------------------+     +---------------------+
                                                              |
+------------------+     +---------------------+              |
| 资源调优配置     | --> | injectResourceSpec()|<-------------+
|   (JSON)         |     |    (算子级注入)     |
+------------------+     +---------------------+
                                |
                                v
                       +---------------------+
                       | executeInternal()   |
                       |   (反射提交作业)    |
                       +---------------------+
```

关键设计决策：
- **DDL 立即执行** —— 脚本切分过程中立即执行 DDL，因为后续语句可能依赖 Catalog 状态。
- **DML 延迟执行** —— 将 DML 批量解析、编译、翻译，以便在提交前向 `Transformation` DAG 注入资源规格。
- **通过 SlotSharingGroup 注入资源** —— 绕过 Flink 的 `isPartialResourceConfigured()` 检查，实现 selective 的算子级资源调优。

---

## 能力边界

**它是什么：**
- 一个生产级的 Flink SQL Application 模板。
- 一座桥梁，连接 Flink SQL 脚本与外部元数据及细粒度资源控制。

**它不是什么：**
- **不是** Flink SQL Gateway —— 遵循 `flink run` 作业提交范式，而非交互式 Gateway 模式。
- **不是** 工具类库 —— 它是一个带 `main()` 方法的**应用模板**，不是一个引入依赖即可使用的工具包。

**边界承诺：**
- 零修改 Flink 引擎、Planner 或 SQL 语义。
- 不提供任何自定义 SQL 方言 —— 执行结果与原生 Flink SQL 完全一致。
- 用户配置的 Flink 参数原样透传，不做任何改动。

---

## 如何贡献

欢迎贡献！详见 [CONTRIBUTING_CN.md](CONTRIBUTING_CN.md)。

---

## 文档

| 文档 | 说明 |
|------|------|
| [CONTRIBUTING_CN.md](CONTRIBUTING_CN.md) | 贡献指南 —— 环境搭建、代码风格、PR 流程 |
| [CHANGELOG_CN.md](CHANGELOG_CN.md) | 版本历史与更新记录 |
| [docs/CAPABILITIES_CN.md](docs/CAPABILITIES_CN.md) | 能力边界 —— 本项目做什么、不做什么 |
| [CONTEXT_CN.md](CONTEXT_CN.md) | 领域术语表 |

---

## 许可证

基于 Apache License, Version 2.0 开源。
详见 [LICENSE](LICENSE)。
