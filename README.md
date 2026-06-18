<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

<h1 align="center">Flink SQL Bootstrap</h1>

<p align="center">
  Your production Flink SQL deployment might just need: one 3MB JAR
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/Java-11%2B-orange" alt="Java">
  <img src="https://img.shields.io/badge/Flink-1.20%2B-brightgreen" alt="Flink">
</p>


## Table of Contents

- [Why Flink SQL Bootstrap](#why-flink-sql-bootstrap)
- [Core Capabilities](#core-capabilities)
- [Quick Start](#quick-start)
  - [Requirements](#requirements)
  - [Step 1 — Run a SQL Script](#step-1--run-a-sql-script)
  - [Step 2 — Generate & Inject Resource Config](#step-2--generate--inject-resource-config)
  - [Step 3 — Deploy with Catalog Snapshot](#step-3--deploy-with-catalog-snapshot)
- [Execution Modes](#execution-modes)
- [Configuration Reference](#configuration-reference)
  - [Resource Hint JSON](#resource-hint-json)
  - [Catalog Snapshot JSON](#catalog-snapshot-json)
- [Capability Boundaries](#capability-boundaries)
- [Contributing](#contributing)
- [Documentation](#documentation)
- [License](#license)


## Why Flink SQL Bootstrap

Native Flink SQL has three pain points in production:

| Pain Point | Native Flink SQL | Flink SQL Bootstrap |
|-----------|------------------|---------------------|
| **Catalog Reusability** | DDL is tightly coupled with the job; tables must be rebuilt on every submission. | Deploy with a **Catalog snapshot JSON** — tables, views, and UDFs are pre-registered, no DDL needed at runtime. |
| **Multi-Statement Script** | Only single-statement submission is supported natively. | Full **Multi-Statement SQL Script** support — DDLs execute immediately, DMLs are compiled, optimized, and submitted as a single pipeline. |
| **Fine-Grained Resource Tuning** | Only TM/JM-level coarse-grained configuration (memory, slots). | **Per-operator resource injection** — CPU, heap memory, managed memory, parallelism, and chaining strategy for each operator. |


## Core Capabilities

1. **Custom Catalog Snapshots** — Serialize tables, views, and UDFs into a JSON file. The catalog is ready at job startup, so no DDL is needed in the SQL script. Supports multi-protocol snapshot loading for streamlined real-time data warehouse metadata management.
2. **Multi-Statement SQL Script** — Write `CREATE TABLE`, `SET`, `INSERT`, and `CALL` statements in a single `.sql` file. The launcher splits, validates, and orchestrates execution automatically.
3. **Fine-Grained Resource Tuning** — Generate a resource template via `--init-resource`, tune parallelism and resources per operator, and inject them into the Flink DAG before job submission.
4. **Universal Protocol Support** — Load SQL scripts and configs from `classpath:`, `file://`, `http(s)://`, `hdfs://`, or `s3://`.
5. **All Flink Deployment Modes** — Works with Standalone, Per-Job, Application, and Session modes on Local, YARN, and Kubernetes.
6. **SQL Validation with Line-Level Diagnostics** — Validate SQL syntax via `--validate` without submitting to a Flink cluster. Errors are reported with exact line and column numbers for rapid iteration.
7. **Flink 1.20+ Compatible** — Works with Flink 1.20 and later without modifying the Flink engine or Planner. (More versions will be supported in the future.)


## Flink Version Compatibility

Verified via the [compatibility test suite](docs/flink-compatibility-test.md) on Local, YARN (docker-compose), and Kubernetes (kind). All modes tested with UDF dependencies, Catalog snapshots, and resource hints.

| Flink Version | Local | YARN-App | YARN-Session | K8s-Session | K8s-App |
|---------------|:-----:|:--------:|:------------:|:-----------:|:-------:|
| 1.20.4 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2.0.2 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2.1.1 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2.2.0 | ✅ | ✅ | ✅ | ✅ | ✅ |

- **✅ PASS** — JAR submits and executes successfully.
- **❌ FAIL** — API incompatibility prevents job submission or execution.
- **—** — Not yet tested (NT).

Full report: [docs/flink-compat-test-1.0.1.html](https://tonyabasy.github.io/flink-sql-bootstrap/flink-compat-test-1.0.1.html)


## Quick Start

### Requirements

| Dependency | Version |
|------------|---------|
| Java | 11+ |
| Flink | 1.20+ |

> **Preparation**: This project depends on `flink-sql-gateway-*.jar`. Before running, copy it from `$FLINK_HOME/opt` to `$FLINK_HOME/lib`:
>
> ```bash
> cp $FLINK_HOME/opt/flink-sql-gateway-*.jar $FLINK_HOME/lib
> ```

### Download

Download the latest JAR from [GitHub Releases](https://github.com/tonyabasy/flink-sql-bootstrap/releases).

### Step 1 — Run a SQL Script

The simplest way — just submit a SQL script:

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file classpath:example-word-count.sql
```

Where `example-word-count.sql` contains **DDL + DML** with zero external dependencies:

```sql
-- datagen auto-generates sentences → split words → count → print output
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

After submission, you'll see output similar to (values vary since datagen generates random data):

```
Flink SQL> INSERT INTO sink_table
           SELECT word, COUNT(*) AS cnt ...
> +I[<random_hex_string>, 1]
> +I[<random_hex_string>, 1]
> +I[<random_hex_string>, 1]
```

### Step 2 — Generate & Inject Resource Config

Generate a resource template from the SQL script:

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file classpath:example-word-count.sql \
    --init-resource
```

Output:

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

The generated template gives each operator a `uid`, `name`, default `parallelism`, and `chainStrategy`. Edit these fields to tune parallelism and chaining, then add `resource` entries for CPU/memory (see [Resource Hint JSON](#resource-hint-json) for available fields).

Save the output as `resource.json`, tune the values, and submit with resources:

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file classpath:example-word-count.sql \
    --resource-file classpath:resource.json
```

### Step 3 — Deploy with Catalog Snapshot

Pre-register tables and UDFs via a Catalog snapshot so the SQL script contains **zero DDL**. Use the companion SQL script designed for this:

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file classpath:example-word-count-advanced.sql \
    --catalog-file classpath:example-catalog.json \
    --resource-file classpath:example-resource.json \
    --dependency classpath:example-udf-reverse.jar \
    --dependency classpath:example-udf-substring.jar
```

Where `example-word-count-advanced.sql` contains only DML (tables and UDFs are pre-registered by the Catalog snapshot):

```sql
INSERT INTO dws_word_count
SELECT my_reverse(my_substring(word, 0, 2)) AS word, COUNT(*) AS cnt
FROM ods_words
CROSS JOIN UNNEST(SPLIT(sentence, ' ')) AS t(word)
GROUP BY my_reverse(my_substring(word, 0, 2));
```

Output (note the 2-character prefixes — the `my_reverse(my_substring(...))` UDF is taking effect):

```
Job has been submitted with JobID <job_id>
+I[6a, 1]
+I[00, 1]
+I[a3, 1]
+I[8f, 1]
```


## Execution Modes

Besides full execution, three dry-run modes are provided for CI/CD and development workflows:

| Mode | Flag | Description |
|------|------|-------------|
| **Validate** | `--validate` | Parse and validate SQL syntax without compiling or executing. |
| **Compile** | `--compile` | Parse, validate, and compile the SQL script. Outputs the optimized plan as JSON. |
| **Init Resource** | `--init-resource` | Translate the SQL plan and output a resource configuration template for tuning. |

```bash
# Validate SQL syntax
$FLINK_HOME/bin/flink run ... --script-file job.sql --validate

# Compile and inspect the execution plan
$FLINK_HOME/bin/flink run ... --script-file job.sql --compile

# Generate resource template
$FLINK_HOME/bin/flink run ... --script-file job.sql --init-resource
```


## Configuration Reference

### Resource Hint JSON

Describes per-operator resources. Each operator is matched by `uid` (preferred) or `name`.

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
        "profile": "stateless"
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

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `version` | int | Schema version, currently `1`. |
| `defaultParallelism` | int | Global default parallelism. `0` means no override. |
| `operators` | array | List of operator configurations. |
| `operators[].uid` | string | Stable UID for matching. Overrides Flink auto-generated UIDs. |
| `operators[].name` | string | Operator name as fallback for matching. |
| `operators[].parallelism` | int | Operator parallelism. `-1` means Flink default. |
| `operators[].chainStrategy` | string | `HEAD`, `ALWAYS`, or `NEVER`. Controls operator chaining. |
| `operators[].resource.profile` | string | Preset: `stateless`, `stateful`, `join_heavy`, `sink`. |
| `operators[].resource.cpu` | double | CPU cores (fractional allowed). |
| `operators[].resource.heap` | string | Task heap memory, e.g. `"512 MB"`, `"2g"`. |
| `operators[].resource.managed` | string | Managed memory, e.g. `"256m"`. |

### Catalog Snapshot JSON

Describes a self-contained catalog with tables, views, and UDFs. Supports computed columns, primary keys, watermarks, partition keys, and metadata columns.

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
        { "name": "sentence", "type": "STRING", "nullable": true },
        { "name": "ts", "type": "TIMESTAMP_LTZ(3)", "nullable": false,
          "isComputed": true, "computedExpr": "PROCTIME()" }
      ],
      "primaryKey": { "columnNames": ["id"], "enforced": true },
      "watermark": { "rowtimeColumn": "ts", "expression": "ts - INTERVAL '5' SECOND" },
      "options": {
        "connector": "datagen",
        "rows-per-second": "1"
      }
    }
  ],
  "views": [
    {
      "database": "default",
      "name": "v_latest_words",
      "expandedQuery": "SELECT sentence FROM ods_words WHERE ts > CURRENT_TIMESTAMP - INTERVAL '10' MINUTE"
    }
  ],
  "udfs": [
    {
      "database": "default",
      "name": "my_reverse",
      "kind": "SCALAR",
      "className": "examples.udf.MyReverseFunction",
      "functionLanguage": "JAVA",
      "jarRef": "example-udf-reverse.jar"
    }
  ]
}
```

> **Note**: The UDF `jarRef` is for lineage tracking only. UDF JARs are loaded via `--dependency` or `pipeline.classpaths` — they are **not** loaded based on this field.

## Capability Boundaries

**What it is:**
- A production-grade Flink SQL Application Template.
- A launcher that bridges Flink SQL scripts with external metadata and fine-grained resource control.

**What it is not:**
- **Not** Flink SQL Gateway — it follows the `flink run` job submission paradigm, not the interactive Gateway pattern.
- **Not** a utility library — it is an **application template** with a `main()` method, not a dependency you import.

**Boundaries:**
- Zero modifications to the Flink engine, Planner, or SQL semantics.
- No custom SQL dialects — you get exactly the same results as native Flink SQL.
- User Flink configurations are passed through as-is.

## License

Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for details.
