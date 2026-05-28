<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

# Flink SQL Bootstrap

A production-grade extension for Flink SQL that provides offline catalog snapshots, a decoupled SQL execution engine, and per-operator resource tuning.

## Why Bootstrap

Vanilla Flink SQL has three pain points in production:

1. **Catalog is not reusable** — DDL is coupled to the job. Tables must be recreated on every submission, and there's no way to integrate with third-party metadata services.
2. **Closed execution path** — `ScriptExecutor` bundles translate → execute into an atomic operation, leaving no room for external intervention.
3. **Coarse resource tuning** — Only TM/JM-level configuration is available. Per-operator CPU, memory, and chaining strategy are out of reach.

Flink SQL Bootstrap opens up these three choke points in the Flink SQL Gateway execution chain.

## Quick Start

```bash
# Build from source
mvn package -DskipTests

# Submit a job (with catalog snapshot + resource config)
flink run target/flink-sql-bootstrap.jar \
    --script-file hdfs:///jobs/order_analysis.sql \
    --catalog-file hdfs:///catalogs/platform-catalog.json \
    --resource-file hdfs:///configs/resource-hint.json \
    --deps hdfs:///udfs/my-udfs.jar
```

## Three Capabilities

### 1. Offline Catalog Snapshots

Serialize a Flink Catalog into a self-contained JSON snapshot. Jobs load it directly at startup — no external metadata source needed.

<details>
<summary>Example catalog snapshot</summary>

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
      "comment": "Active users in the past 7 days",
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

**Supported file sources:**

| Scheme | Description |
|--------|-------------|
| `file://` / absolute path | Local filesystem |
| `hdfs://` | HDFS / any Flink FileSystem |
| `http://` / `https://` | HTTP API (integrate with third-party metadata services) |
| `classpath://` | Resources bundled in the JAR |

Usage:

```bash
# Local JSON file
--catalog-file /path/to/catalog.json

# HTTP API (for external metadata centers)
--catalog-file http://metadata-api.example.com/catalogs/123

# Inline JSON
--catalog '{"version":1,"catalogName":"test",...}'
```

Catalog snapshots are **optional** — omit them and DDL statements in the SQL script will create tables dynamically. Fully backward-compatible.

### 2. Decoupled SQL Execution Engine

Rebuilt on top of Flink SQL Gateway's `ScriptExecutor`. Replaces "split-and-execute" with a four-stage pipeline: **split → parse → transform → resource injection → execute**.

```
Traditional ScriptExecutor:
  SQL → [split] → executeStatement() one-by-one → no intervention possible

Boost StreamingScriptExecutor:
  SQL → [split + parse] → [DDL: execute immediately, DML: hold] → [planner.translate()] → [resource injection] → [reflective submit]
                                                                    ↑                           ↑
                                                             DDL affects later parsing      intervention window
```

**Key changes:**

- **DDL/DML separation** — DDL (`CREATE TABLE`, `SET`, etc.) executes immediately to maintain catalog state. DML (`INSERT`, `STATEMENT SET`) is deferred until after resource injection.
- **Reflective execution** — Caches the `Method` reference for `TableEnvironmentImpl.executeInternal()` to avoid class lookup overhead.
- **Supports three Flink deployment modes** — Application Mode, Session Mode, Local Mode.

### 3. Per-Operator Resource Tuning

Precisely control parallelism, CPU, memory, and chaining strategy for each operator via JSON configuration.

<details>
<summary>Example resource configuration</summary>

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

**Two ways to specify resources:**

| Method | Example | Description |
|--------|---------|-------------|
| Preset profile | `{"profile": "large"}` | small / normal / large / xlarge, auto-expanded to CPU + memory |
| Explicit values | `{"cpuCores": 1.0, "heapMemory": "2048m"}` | Fine-grained control |

**Preset profiles:**

| Profile | CPU | Heap | Managed Memory |
|---------|-----|------|----------------|
| small | 0.25 | 512 MB | — |
| normal | 0.5 | 1 GB | — |
| large | 1.0 | 2 GB | 256 MB |
| xlarge | 2.0 | 4 GB | 512 MB |

**Operator matching strategy:**

1. **UID match** (preferred) — exact match via `Transformation.getUid()`
2. **Name match** (fallback) — match via `Transformation.getName()`

**Chaining strategies:**

| Value | Effect |
|-------|--------|
| `ALWAYS` | Chain with upstream whenever possible |
| `NEVER` | Never chain (force independent slot) |
| `HEAD` | Chain with upstream only, not with downstream |

**Injection mechanism:** Uses `SlotSharingGroup` to convey resources rather than `setResources()`, avoiding Flink's `isPartialResourceConfigured()` check that would block local mode submission. Operators with the same resource config automatically share an SSG, preserving operator chains.

## CLI Reference

```
flink run [flink-options] <jar> \
    [--script-file <file> | --script <sql>] \
    [--resource-file <file> | --resource <json>] \
    [--catalog-file <file> | --catalog <json>] \
    [--deps <jar1>,<jar2>,...]
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| `--script` | `-s` | Inline SQL script content |
| `--script-file` | `-sf` | SQL file path (supports file/hdfs/http/classpath) |
| `--resource` | `-r` | Inline resource config JSON |
| `--resource-file` | `-rf` | Resource config file path |
| `--catalog` | `-c` | Inline catalog snapshot JSON |
| `--catalog-file` | `-cf` | Catalog snapshot file path |
| `--deps` | — | Dependency JAR paths (equivalent to `flink run -C`) |

**Each `--xxx` / `--xxx-file` pair is mutually exclusive.** In Application Mode, absolute local paths are not available — use `--script`/`--resource` for inline values or ship files into the container.

## Project Structure

```
com.lanting.flink.sql.bootstrap/
├── SqlEntryPoint.java              # CLI entry point: argument parsing + orchestration
├── ClassUtils.java                 # ClassLoader utilities
├── executor/
│   └── StreamingScriptExecutor.java # Core executor: SQL split → parse → translate → resource injection → execute
├── catalog/
│   ├── CatalogEntity.java          # Catalog snapshot root model
│   ├── CatalogEntityFactory.java   # Snapshot → GenericInMemoryCatalog
│   ├── TableEntity.java            # Table definition
│   ├── ColumnEntity.java           # Column definition (physical / computed / metadata)
│   ├── ViewEntity.java             # View definition
│   ├── UdfEntity.java              # UDF definition
│   ├── PrimaryKeyEntity.java       # Primary key definition
│   └── WatermarkEntity.java        # Watermark definition
├── resource/
│   ├── ResourceEntity.java         # Resource config root model
│   ├── OperatorSpec.java           # Per-operator configuration
│   └── OperatorResourceSpec.java   # CPU + memory spec (including presets)
└── flink/
    └── UriSafeSessionContext.java  # Fix for FLINK-39687 URI→URL conversion
```

## Build

```bash
# Compile
mvn compile

# Run tests
mvn test

# Package
mvn package -DskipTests
```

**Requirements:** Java 11+, Flink 2.2.0 (all Flink dependencies are `provided` scope).

## UID Stability Tests

The project includes 20 parameterized tests covering common SQL iteration scenarios (adding/removing sources and sinks, JOIN changes, window functions, CTE rewrites, partition pushdown, etc.). Each compares operator UID stability between `before.sql` and `after.sql`. See [docs/uid-generation.md](docs/uid-generation.md).

```bash
mvn test -Dtest=UidStabilityTest
```

## License

Apache License, Version 2.0
