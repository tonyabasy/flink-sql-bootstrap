<div class="hero">

# Flink SQL Bootstrap

<p class="tagline">Your production Flink SQL deployment might just need: one 3MB JAR</p>

<div class="badges">
  <span class="badge badge-green">Flink 1.20+</span>
  <span class="badge badge-blue">Apache 2.0</span>
  <span class="badge badge-orange">Java 11+</span>
</div>

</div>

## Why not native Flink SQL CLI?

Native Flink SQL is powerful but has three gaps in production:

| Pain Point | Native Flink SQL | Flink SQL Bootstrap |
|:-----------|:-----------------|:--------------------|
| **Catalog Reusability** | DDL tightly coupled; tables rebuilt on every submission | Catalog snapshot JSON — tables, views, UDFs pre-registered, zero DDL at runtime |
| **Multi-Statement Scripts** | Only single-statement submission natively | Full multi-statement support — DDLs run immediately, DMLs compiled as a single pipeline |
| **Resource Tuning** | Only TM/JM-level coarse-grained config | Per-operator injection — CPU, heap, managed memory, parallelism, chaining strategy |

## Quick Start

```
# Requirements: Java 11+, Flink 1.20+
# 1. Copy flink-sql-gateway jar:
cp $FLINK_HOME/opt/flink-sql-gateway-*.jar $FLINK_HOME/lib

# 2. Download latest JAR from GitHub Releases:
#    https://github.com/tonyabasy/flink-sql-bootstrap/releases

# 3. Run:
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file my_job.sql
```

[Full guide →](guide.md)

---

<div class="section-title">Core Capabilities</div>

<div class="features">

<div class="feature">
<h4>📦 Catalog Snapshots</h4>
<p>Serialize tables, views, and UDFs into a JSON file. No DDL needed in your SQL script.</p>
</div>

<div class="feature">
<h4>📝 Multi-Statement SQL</h4>
<p>Write DDL, DML, SET, and CALL in one .sql file. Auto-splits, validates, and orchestrates.</p>
</div>

<div class="feature">
<h4>⚙️ Fine-Grained Resources</h4>
<p>Tune CPU, memory, parallelism, and chaining per operator. Generate templates with --init-resource.</p>
</div>

<div class="feature">
<h4>🌐 Universal Protocols</h4>
<p>Load SQL scripts and configs from classpath:, file://, http(s)://, hdfs://, or s3://,</p>
</div>

<div class="feature">
<h4>🚀 All Deployment Modes</h4>
<p>Works with Local, YARN (App/Session), and Kubernetes (App/Session).</p>
</div>

<div class="feature">
<h4>🔍 SQL Validation</h4>
<p>Validate syntax with --validate. Errors reported with exact line and column numbers.</p>
</div>

</div>

---

<div class="section-title">News</div>

<div class="home-list">

<div class="item">
  <span class="date">2026-06-16</span>
  <a href="blogs/02-cicd-pipeline.md">Flink in Production: CI/CD Pipeline Like a Backend Service</a>
</div>

<div class="item">
  <span class="date">2026-06-10</span>
  <a href="blogs/01-hive-like-flink-sql.md">Flink in Production: Use Flink SQL Like Hive</a>
</div>

<div class="item">
  <span class="date">2026-06-08</span>
  <a href="https://github.com/tonyabasy/flink-sql-bootstrap/releases/tag/v1.0.0">v1.0.0 Released</a>
  &mdash; Initial release: multi-statement SQL, Catalog snapshots, resource tuning
</div>

</div>

<div class="site-footer">
  <a href="https://github.com/tonyabasy/flink-sql-bootstrap">GitHub</a> &nbsp;·&nbsp;
  Licensed under <a href="https://github.com/tonyabasy/flink-sql-bootstrap/blob/main/LICENSE">Apache 2.0</a>
</div>
