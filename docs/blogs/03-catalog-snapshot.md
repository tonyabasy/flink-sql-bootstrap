# Flink 实时数仓开发实战：Catalog 快照 — 告别 DDL 地狱

> **系列**：Flink 实时数仓开发实战 · 第二篇
> **关键词**：Catalog 快照、元数据管理、DDL 解耦、多协议加载、UDF 管理
> **适合人群**：被 Flink SQL 的 DDL 重复劳动折磨过的开发者

---

## 引言：DDL 地狱

上一篇文章我们实现了"一条脚本跑 Flink SQL"，但还有一个问题：**DDL 仍然嵌在 SQL 脚本里**。

这意味着什么？

- **改一个字段，要改所有引用这张表的 SQL 脚本**
- **多作业共享同一张表，DDL 在每个脚本里各写一遍**
- **UDF 的类名、JAR 路径，散落在不同脚本中**

这在 Hive 世界里根本不是问题——Hive Metastore 统一管着所有表的元数据，你的 `SELECT` 语句里不需要写 `CREATE TABLE`。但 Flink SQL 在 Application Mode 下提交时，每个作业都是独立进程，无法依赖一个全局 Metastore。

**Catalog 快照就是为这个场景设计的**。它把 DDL 从 SQL 脚本中剥离出来，序列化为一个独立的 JSON 文件，作业启动时自动恢复到内存 Catalog。SQL 脚本从此只写业务逻辑，不写 DDL。

---

## 原型：一个完整的 Catalog 快照

来看 Flink SQL Bootstrap 自带的示例 Catalog 快照 `example-catalog.json`：

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
    },
    {
      "database": "default",
      "name": "dws_word_count",
      "columns": [
        { "name": "word", "type": "STRING", "nullable": false },
        { "name": "cnt", "type": "BIGINT", "nullable": false }
      ],
      "options": {
        "connector": "print"
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
    },
    {
      "name": "my_substring",
      "className": "examples.udf.MySubstringFunction",
      "functionLanguage": "JAVA",
      "jarRef": "example-udf-substring.jar"
    }
  ]
}
```

配合一个**只含 DML** 的 SQL 脚本 `example-word-count-advanced.sql`：

```sql
INSERT INTO dws_word_count
SELECT my_reverse(my_substring(word, 0, 2)) AS word, COUNT(*) AS cnt
FROM ods_words
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY my_reverse(my_substring(word, 0, 2));
```

注意这个 SQL 脚本里**没有任何 DDL**——没有 `CREATE TABLE`，没有 `CREATE FUNCTION`。表 `ods_words`、`dws_word_count` 和 UDF `my_reverse`、`my_substring` 全部来自 Catalog 快照。

部署命令：

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count-advanced.sql \
    --catalog-file classpath:example-catalog.json \
    --dependency classpath:example-udf-reverse.jar \
    --dependency classpath:example-udf-substring.jar
```

执行结果证明一切正常——UDF `my_reverse` 和 `my_substring` 的组合效果在输出中清晰可见：

```
+I[6a, 1]
+I[00, 1]
+I[a3, 1]
+I[8f, 1]
```

---

## 原理：Catalog 是如何"冻结"和"解冻"的？

### 设计哲学：自包含快照

Catalog 快照的核心思想很简单：**把 Flink 内存 Catalog 的完整状态序列化为一个 JSON 文件，运行时再反序列化恢复**。

这个 JSON 是**自包含的**——它不依赖任何外部元数据中心（如 Hive Metastore）。你可以把这个 JSON 文件放在：

- **classpath**：`classpath:orders-catalog.json`
- **本地文件系统**：`file:///data/catalogs/orders-catalog.json`
- **HTTP/HTTPS**：`https://config-center.company.com/catalogs/prod-orders.json`
- **HDFS**：`hdfs://namenode/flink/catalogs/orders-catalog.json`
- **S3**：`s3://bucket/flink/catalogs/orders-catalog.json`

这五种协议覆盖了从本地开发到云端部署的所有场景。

### "解冻"流程：`CatalogEntityFactory` 的恢复四步走

当作业启动时，`SqlEntryPoint` 读取 Catalog JSON，交给 `CatalogEntityFactory` 恢复：

**第一步：创建内存 Catalog**

```java
GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(catalogName, databaseName);
```

`GenericInMemoryCatalog` 是 Flink 内置的内存 Catalog 实现，速度快、零依赖。

**第二步：确保数据库存在**

```java
catalog.createDatabase(databaseName, new CatalogDatabaseImpl(...), false);
```

**第三步：注册表**

`CatalogEntityFactory.toCatalogTable()` 将 `TableEntity` 转换为 Flink 的 `CatalogTable`。关键点在于 Schema 的构建顺序：

```
物理列（Physical Columns）
  → 元数据列（Metadata Columns）
  → 计算列（Computed Columns）
  → Watermark 定义
  → 主键约束
```

这个顺序必须严格遵循 Flink 的 Schema 构建规范，否则恢复后查询可能报错。

**第四步：注册 UDF**

```java
tableEnv.createTemporarySystemFunction(udfName, udfClass);
```

UDF 的 class 从 `--dependency` 指定的 JAR 中加载——Catalog 快照只记录类名和 JAR 引用，不存储字节码。

### 完整的"解冻"后的架构图

```
+---------------------+       +-----------------------+
|  catalog.json        | ----> | CatalogEntityFactory  |
|  (tables+views+udfs) |       |  (四步恢复)           |
+---------------------+       +-----------------------+
                                        |
                                        v
+---------------------+       +-----------------------+
|  SQL 脚本 (纯 DML)   | ----> | GenericInMemoryCatalog|
|  无任何 DDL 语句      |       |  (表+视图+UDF 已就绪) |
+---------------------+       +-----------------------+
                                        |
                                        v
                              +-----------------------+
                              |  Flink Planner        |
                              |  (编译 DML → Job)      |
                              +-----------------------+
```

---

## 实战：为实时数仓构建 Catalog 快照

让我们基于第一篇的订单数仓场景，构建对应的 Catalog 快照。

### 快照内容

```json
{
  "version": 1,
  "snapshotId": "realtime-orders-v2",
  "catalogName": "realtime_dw",
  "databaseName": "default",
  "tables": [
    {
      "database": "default",
      "name": "ods_orders",
      "columns": [
        { "name": "order_id", "type": "BIGINT", "nullable": false },
        { "name": "user_id", "type": "BIGINT", "nullable": false },
        { "name": "amount", "type": "DECIMAL(10, 2)", "nullable": true },
        { "name": "status", "type": "STRING", "nullable": true },
        { "name": "event_time", "type": "TIMESTAMP(3)", "nullable": true },
        { "name": "ts", "type": "TIMESTAMP_LTZ(3)", "nullable": true, "isComputed": true, "computedExpr": "PROCTIME()" }
      ],
      "options": {
        "connector": "kafka",
        "topic": "orders_raw",
        "properties.bootstrap.servers": "${kafka.brokers}",
        "format": "json",
        "scan.startup.mode": "latest-offset"
      }
    },
    {
      "database": "default",
      "name": "dwd_orders",
      "columns": [
        { "name": "order_id", "type": "BIGINT", "nullable": false },
        { "name": "user_id", "type": "BIGINT", "nullable": false },
        { "name": "amount", "type": "DECIMAL(10, 2)", "nullable": true },
        { "name": "status", "type": "STRING", "nullable": true },
        { "name": "dt", "type": "STRING", "nullable": true }
      ],
      "primaryKey": {
        "name": "pk_dwd_orders",
        "columns": ["order_id"],
        "enforced": false
      },
      "options": {
        "connector": "upsert-kafka",
        "topic": "dwd_orders",
        "properties.bootstrap.servers": "${kafka.brokers}",
        "key.format": "json",
        "value.format": "json"
      }
    },
    {
      "database": "default",
      "name": "ads_user_amount",
      "columns": [
        { "name": "user_id", "type": "BIGINT", "nullable": false },
        { "name": "total_amount", "type": "DECIMAL(15, 2)", "nullable": true },
        { "name": "order_cnt", "type": "BIGINT", "nullable": true }
      ],
      "primaryKey": {
        "name": "pk_ads_user",
        "columns": ["user_id"],
        "enforced": true
      },
      "options": {
        "connector": "jdbc",
        "url": "${mysql.url}",
        "table-name": "ads_user_amount",
        "username": "${mysql.user}",
        "password": "${mysql.password}"
      }
    }
  ],
  "views": [],
  "udfs": [
    {
      "name": "normalize_status",
      "className": "com.company.udf.NormalizeStatusFunction",
      "functionLanguage": "JAVA",
      "jarRef": "udf-common.jar"
    }
  ]
}
```

### 此时 SQL 脚本变成什么样了？

```sql
-- 只保留 SET 和 DML，DDL 全部在 Catalog 快照中
SET 'pipeline.name' = 'realtime-orders-warehouse';
SET 'execution.checkpointing.interval' = '30s';

INSERT INTO dwd_orders
SELECT
    order_id,
    user_id,
    amount,
    normalize_status(status) AS status,
    DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt
FROM ods_orders
WHERE status IN ('PAID', 'SHIPPED');
```

干净，清爽，纯业务逻辑。

---

## 最佳实践

### 1. 元数据版本化

Catalog 快照 JSON 和你改 DDL 表结构一样应该进 Git。`snapshotId` 字段用于标记版本：

```json
{
  "snapshotId": "realtime-orders-v3",
  ...
}
```

每次修改表结构时更新 `snapshotId`，配合 Git Tag，可以快速回溯任意时刻的 Catalog 状态。

### 2. 环境差异化

正式环境的 Kafka 地址、MySQL 密码不应硬编码。在 Catalog 快照中使用占位符（如 `${kafka.brokers}`），部署时用脚本做变量替换，或者**将不同环境的快照版本独立管理**：

```
catalogs/
├── dev/
│   └── orders-catalog.json      # dev 环境
├── staging/
│   └── orders-catalog.json      # staging 环境
└── prod/
    └── orders-catalog.json      # 生产环境
```

### 3. UDF 依赖管理

Catalog 快照中的 `jarRef` 是逻辑名，部署时通过 `--dependency` 指定实际 JAR 路径：

```bash
flink run ... \
    --catalog-file s3://.../orders-catalog.json \
    --dependency s3://jars/udf-common-v2.1.jar
```

这实现了 UDF 版本与 Catalog 快照的独立演化——改 UDF 不需要重写快照。

### 4. 快照即文档

Catalog 快照天然就是你的数据字典。配合一个 JSON Schema 校验器，可以在 CI 中自动检查：

- 字段名是否遵循命名规范
- 连接器配置是否完整
- UDF 引用是否存在对应的 JAR

### 5. 多协议加载的部署策略

| 协议 | 适用场景 |
|------|---------|
| `classpath:` | 开发环境、快速 demo |
| `file://` | 本地调试、单机部署 |
| `http(s)://` | 配置中心统一分发、CI/CD |
| `hdfs://` | 传统 Hadoop 集群 |
| `s3://` | 云原生部署 |

生产环境推荐 `http(s)://` 或 `s3://`，结合配置中心的版本控制能力，实现快照的自动下发和回滚。

---

## 小结

Catalog 快照解决了 Flink SQL 在生产环境中最令人头疼的问题之一：**DDL 与业务逻辑的耦合**。它将表、视图、UDF 的元数据从 SQL 脚本中独立出来，实现了：

1. **一处定义，处处复用**——多作业共享同一份元数据
2. **DDL 变更零影响**——改字段只需改一份 JSON
3. **环境隔离**——不同环境独立快照
4. **多协议加载**——适配任何部署环境

下一篇，我们将探讨 **如何像后端项目一样管理 Flink SQL 工程**——目录规范、版本控制、Code Review、CI/CD 一体化。

---

*本文基于 [Flink SQL Bootstrap](https://github.com/tonyabasy/flink-sql-bootstrap) v1.0.0*
