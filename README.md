<p align="center">
  <a href="README.md">English</a> |
  <a href="README_CN.md">中文</a>
</p>

# Flink SQL Bootstrap

A production-grade enhanced launcher for Flink SQL Application Template, delivering three core capabilities: custom Catalog snapshots, Multi-Statement SQL Script deployment, and fine-grained resource tuning.

## Problems It Solves

Three pain points in native Flink SQL:

1. **Catalog is not reusable** — DDL is tightly coupled with the job; tables must be rebuilt on every submission, with no way to integrate external metadata.
2. **No Multi-Statement SQL Script support** — SQL statements can only be executed one at a time; there is no way to orchestrate, validate, and submit multiple statements as a single unit.
3. **No fine-grained resource tuning** — Fine-grained resource tuning offers enormous potential for resource optimization, but is not supported by official Flink SQL.

Flink SQL Bootstrap is built specifically to address these three problems.

## Capability Boundaries

See [docs/CAPABILITIES.md](docs/CAPABILITIES.md).

## Quick Start

### Requirements

| Dependency | Version |
|------------|---------|
| Java | 11+ |
| Flink | 1.20.x / 2.2.0 |

### Submitting a Job

```bash
# Build
mvn package -DskipTests

# Submit with all options
$FLINK_HOME/bin/flink run \
    --target local \
    -Dpipeline.name="My First SQL Job" \
    /path/to/flink-sql-bootstrap.jar \
    --script-file classpath:example-word-count.sql \
    --resource-file classpath:example-resource.json \
    --catalog-file classpath:example-catalog.json \
    --dependency classpath:example-udf-reverse.jar \
    --dependency classpath:example-udf-substring.jar

# All file parameters support the following protocols:
#   classpath:  - read from application JAR resources
#   http(s)://  - fetch from remote HTTP server
#   /path/to    - read from local file system
#   hdfs://     - read from HDFS or other DFS via Flink FileSystem
#   s3://       - read from S3 via Flink FileSystem
```

## Contributing

To be added.