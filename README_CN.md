<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

# Flink SQL Bootstrap

Flink SQL 的生产级增强扩展，提供第三方 Catalog 快照、Multi SQL Script 校验、执行能力和 Flink SQL 细粒度资源调优三大能力。

## 为什么需要 Bootstrap

原生 Flink SQL 在生产落地时有三个痛点：

1. **Catalog 不可复用** — DDL 和作业耦合，每次提交都要重新建表，无法对接第三方元数据服务
2. **执行路径封闭** — `ScriptExecutor` 将 translate → execute 封装为原子操作，外部无法介入
3. **资源调优粗糙** — 只能按 TM/JM 整体配置，无法为单个算子指定 CPU、内存、Chain 策略

Flink SQL Bootstrap 在 Flink SQL Gateway 的执行链上打开了这三个卡点。

## 快速开始

```bash
# 从源码构建
mvn package -DskipTests

# 提交作业（带 Catalog 快照 + 资源配置）
flink run target/flink-sql-bootstrap.jar \
    --script-file hdfs:///jobs/order_analysis.sql \
    --catalog-file hdfs:///catalogs/platform-catalog.json \
    --resource-file hdfs:///configs/resource-hint.json \
    --deps hdfs:///udfs/my-udfs.jar
```

## 三大能力

### 1. 离线 Catalog 快照

将 Flink Catalog 序列化为自包含的 JSON 快照，作业启动时直接加载，无需连接外部元数据源。

<details>
<summary>Catalog 快照示例</summary>

```json
{
  "version": 1,
  "snapshotId": "20260528-001",
  "catalogName": "platform",
  "databaseName": "default",
  "flinkVersion": "2.2.0",
  "tables": [
    {
      "database": "default",
      "name": "user_events",
      "columns": [
        { "name": "user_id", "type": "BIGINT", "nullable": false },
        { "name": "event_time", "type": "TIMESTAMP(3)", "nullable": true },
        { "name": "kafka_topic", "type": "STRING", "metadataKey": "topic", "virtual": true }
      ],
      "watermark": {
        "rowtimeColumn": "event_time",
        "expression": "event_time - INTERVAL '5' SECOND"
      },
      "primaryKey": { "columnNames": ["user_id"] },
      "options": {
        "connector": "kafka",
        "topic": "user-events",
        "properties.bootstrap.servers": "broker:9092",
        "format": "json"
      }
    }
  ],
  "views": [
    {
      "database": "default",
      "name": "active_users",
      "comment": "过去 7 天活跃用户",
      "expandedQuery": "SELECT user_id FROM user_events WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY"
    }
  ],
  "udfs": [
    {
      "name": "parse_json_field",
      "className": "com.example.udf.ParseJsonField",
      "database": "default",
      "functionLanguage": "JAVA"
    }
  ]
}
```
</details>

**支持的文件来源：**

| Scheme | 说明 |
|--------|------|
| `file://` / 绝对路径 | 本地文件 |
| `hdfs://` | HDFS / 任何 Flink FileSystem |
| `http://` / `https://` | HTTP API（可对接第三方元数据服务） |
| `classpath://` | JAR 内置资源 |

使用方式：

```bash
# 本地 JSON 文件
--catalog-file /path/to/catalog.json

# HTTP API（适合对接外部元数据中心）
--catalog-file http://metadata-api.example.com/catalogs/123

# 直接内联 JSON
--catalog '{"version":1,"catalogName":"test",...}'
```

Catalog 快照是**可选的** — 不传时由 SQL 脚本中的 DDL 动态建表，完全向后兼容。

### 2. 通用 SQL 执行引擎

在 Flink SQL Gateway 的 `ScriptExecutor` 基础上重构，将「边切边执行」改为「切分 → parse → transform → 资源注入 → execute」四阶段流水线。

```
传统 ScriptExecutor:
  SQL → [切分] → 逐条 executeStatement() → 不可介入

Boost StreamingScriptExecutor:
  SQL → [切分 + parse] → [DDL 立即执行, DML 暂存] → [planner.translate()] → [资源注入] → [反射提交]
                                                     ↑                           ↑
                                                  DDL 影响后续 parse           介入窗口
```

**关键改造点：**

- **DDL 与 DML 分离** — DDL（CREATE TABLE、SET 等）立即执行以维护 catalog 状态，DML（INSERT、STATEMENT SET）延迟到资源注入后统一提交
- **反射执行** — 缓存 `TableEnvironmentImpl.executeInternal()` 的 `Method` 引用，避免类查找开销
- **支持三种 Flink 部署模式** — Application Mode、Session Mode、Local Mode

### 3. 算子级资源调优

通过 JSON 配置精确控制每个算子的并行度、CPU、内存和 Chain 策略。

<details>
<summary>资源配置示例</summary>

```json
{
  "version": 1,
  "defaultParallelism": 2,
  "operators": [
    {
      "uid": "1_source",
      "name": "user_events[1]",
      "parallelism": 2,
      "chainStrategy": "ALWAYS",
      "resource": { "profile": "small" }
    },
    {
      "uid": "4_group-aggregate",
      "name": "GroupAggregate[4]",
      "parallelism": 1,
      "resource": {
        "cpuCores": 1.0,
        "heapMemory": "2048m",
        "managedMemory": "256m"
      }
    },
    {
      "uid": "8_sink",
      "name": "kafka_sink[8]",
      "chainStrategy": "HEAD"
    }
  ]
}
```
</details>

**资源规格支持两种配置：**

| 方式 | 示例 | 说明 |
|------|------|------|
| 预置规格 | `{"profile": "large"}` | small / normal / large / xlarge，自动展开为 CPU + 内存 |
| 显式值 | `{"cpuCores": 1.0, "heapMemory": "2048m"}` | 精确控制 |

**预置规格对照：**

| Profile | CPU | Heap | Managed Memory |
|---------|-----|------|----------------|
| small | 0.25 | 512 MB | — |
| normal | 0.5 | 1 GB | — |
| large | 1.0 | 2 GB | 256 MB |
| xlarge | 2.0 | 4 GB | 512 MB |

**算子匹配策略：**

1. **UID 匹配**（优先）— 通过 `Transformation.getUid()` 精确匹配
2. **名称匹配**（降级）— 通过 `Transformation.getName()` 兜底

**Chain 策略：**

| 值 | 效果 |
|----|------|
| `ALWAYS` | 尽可能和前驱 chain |
| `NEVER` | 绝不 chain（强制独立 slot） |
| `HEAD` | 只和前驱 chain，不和后续 chain |

**注入方式：** 使用 `SlotSharingGroup` 传递资源而非 `setResources()`，避免触发 Flink 的 `isPartialResourceConfigured()` 检查导致本地模式提交失败。同一资源配置的算子自动共享 SSG，保持 operator chain 不被打断。

## CLI 参考

```
flink run [flink-options] <jar> \
    [--script-file <file> | --script <sql>] \
    [--resource-file <file> | --resource <json>] \
    [--catalog-file <file> | --catalog <json>] \
    [--deps <jar1>,<jar2>,...]
```

| 参数 | 简写 | 说明 |
|------|------|------|
| `--script` | `-s` | 直接传入 SQL 脚本内容 |
| `--script-file` | `-sf` | SQL 文件路径（支持 file/hdfs/http/classpath） |
| `--resource` | `-r` | 直接传入资源配置 JSON |
| `--resource-file` | `-rf` | 资源配置文件路径 |
| `--catalog` | `-c` | 直接传入 Catalog 快照 JSON |
| `--catalog-file` | `-cf` | Catalog 快照文件路径 |
| `--deps` | — | UDF 等依赖 JAR 路径（等效 `flink run -C`） |

**每组 `--xxx` 和 `--xxx-file` 互斥**。Application Mode 下不能传本地绝对路径，请使用 `--script`/`--resource` 内联传值或将文件 ship 到容器内。

## 项目结构

```
com.lanting.flink.sql.bootstrap/
├── SqlEntryPoint.java              # CLI 入口，参数解析 + 流程编排
├── ClassUtils.java                 # ClassLoader 工具
├── executor/
│   └── StreamingScriptExecutor.java # 核心执行器：SQL 切分 → parse → translate → 资源注入 → 执行
├── catalog/
│   ├── CatalogEntity.java          # Catalog 快照根模型
│   ├── CatalogEntityFactory.java   # 快照 → GenericInMemoryCatalog
│   ├── TableEntity.java            # 表定义
│   ├── ColumnEntity.java           # 列定义（物理列/计算列/元数据列）
│   ├── ViewEntity.java             # 视图定义
│   ├── UdfEntity.java              # UDF 定义
│   ├── PrimaryKeyEntity.java       # 主键定义
│   └── WatermarkEntity.java        # Watermark 定义
├── resource/
│   ├── ResourceEntity.java         # 资源配置根模型
│   ├── OperatorSpec.java           # 单个算子配置
│   └── OperatorResourceSpec.java   # CPU + 内存规格（含预置规格）
└── flink/
    └── UriSafeSessionContext.java  # 修复 FLINK-39687 URI→URL 转换问题
```

## 构建

```bash
# 编译
mvn compile

# 运行测试
mvn test

# 打包
mvn package -DskipTests
```

**依赖：** Java 11+，Flink 2.2.0（所有 Flink 依赖为 `provided` scope）。

## UID 稳定性测试

项目包含 20 组参数化测试，覆盖常见的 SQL 迭代场景（增减 source/sink、JOIN 变更、窗口函数、CTE 改写、分区下推等），每轮对比 `before.sql` / `after.sql` 的算子 UID 稳定性。详见 [docs/uid-generation.md](docs/uid-generation.md)。

```bash
mvn test -Dtest=UidStabilityTest
```

## License

Apache License, Version 2.0
