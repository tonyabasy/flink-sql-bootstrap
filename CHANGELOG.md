# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-06-19

### Changed

- **Domain-driven resource profile renaming** — Resource profiles renamed from cup-size naming to operator-responsibility naming (`small` → `stateless`, `normal` → `stateful`, `large` → `join_heavy`, `xlarge` → `sink`), with resource values tuned accordingly. **Not backward-compatible with legacy resource configuration JSON**

### Added

- **Documentation site** — Built a documentation site with bilingual architecture, including blogs, user guide, and community pages

## [1.0.0] - 2026-06-08

### Added

- **Multi-Statement SQL Script execution engine** — Supports mixing `CREATE TABLE`, `SET`, `INSERT`, `CALL`, and other statements in a single `.sql` file. Statements are automatically split, validated, and orchestrated in semantic order, with DML statements deferred to the compilation phase for unified submission.
- **Catalog snapshot pre-registration** — Pre-register tables, views, and UDFs via a JSON file so the Catalog is fully ready at job startup, eliminating the need for DDL in the SQL script. Supports multi-protocol loading: `classpath:`, `file://`, `http(s)://`, and `hdfs://`.
- **Fine-grained operator-level resource injection** — Inject parallelism, CPU, heap memory, managed memory, off-heap memory, external resources, and chain strategy at the Transformation DAG level, matched by operator UID or name. Operators with identical resource configs are automatically grouped into the same SlotSharingGroup.
- **SQL syntax validation (`--validate`)** — Validate SQL syntax locally without submitting to a Flink cluster. Parse errors include exact line and column numbers for rapid iteration.
- **SQL compilation and execution plan output (`--compile`)** — Parse, validate, and compile SQL, outputting the `InternalPlan` JSON execution plan without actually submitting the job, useful for preview and debugging.
- **Resource template auto-generation (`--init-resource`)** — Automatically extract the Transformation DAG structure from the current SQL script and generate a per-operator resource configuration JSON template. Users can modify values and inject them directly.
- **Flink 1.20.x / 2.x dual-version compatibility** — Bypass SPI compatibility checks via `ApplicationOperationExecutor`, and fix the `URI→URL` type conversion `ArrayStoreException` via `UriSafeSessionContext`, enabling normal job startup in Application Mode.
- **Deterministic operator UID generation** — Force-enable `TABLE_EXEC_UID_GENERATION = ALWAYS` to ensure every Transformation has a stable UID, serving as the exact match key in the resource configuration JSON.
- **Formatted SQL result printing** — Compatible with Flink 1.20.x/2.x `TableauStyle` result table rendering, with exponential backoff via `RowDataIterator` for polling results.
- **Exception hierarchy** — Define `SqlValidateException`, `SqlCompileException`, and `SqlParsePosException`; SQL parse errors carry source line and column position information.
- **Experimental API marker** — Introduce the `@Experimental` annotation to mark incubating APIs such as the DAG printer.
- **DAG topology visualization (experimental)** — ASCII-art rendering of the Transformation DAG, supporting typical topologies such as dual-source Join, Union, and multi-way aggregation in the console.

### Build & CI

- Maven-based build with Shade Plugin configured for fat JAR packaging; main class is `SqlEntryPoint`.
- Spotless configured for code formatting and automatic Apache 2.0 License Header injection.
- GitHub Actions CI configured with Java 11 / 17 / 21 matrix builds.
- 45 unit tests covering core executor, resource injection, entry-point security, and resource spec signing logic.

### Documentation

- Complete Chinese and English READMEs, including Quick Start, CLI options, and configuration examples.
- Capability boundary documentation ([CAPABILITIES.md](docs/CAPABILITIES.md)).
- Domain glossary ([CONTEXT.md](CONTEXT.md)).
- SQL examples ([`example-word-count.sql`](src/main/resources/example-word-count.sql), [`example-word-count-advanced.sql`](src/main/resources/example-word-count-advanced.sql)).
- Catalog snapshot example ([`example-catalog.json`](src/main/resources/example-catalog.json)).
- Operator resource configuration example ([`example-resource.json`](src/main/resources/example-resource.json)).
- Sample UDF JARs (`example-udf-reverse.jar`, `example-udf-substring.jar`).
- AI Agent collaboration guidelines (`docs/agents/`).
- Flink multi-version compatibility test suite (`scripts/flink-compat-test/`) — automated testing across Flink 1.20 ~ 2.2 in Local, YARN, and Kubernetes deployment modes. Generates HTML compatibility reports (`docs/flink-compat-test-<version>.html`) with pass/fail matrix and error classification.

- All 5 deployment modes (Local, YARN-App, YARN-Session, K8s-Session, K8s-App) tested and passing.
- Full compatibility report: [docs/flink-compat-test-1.0.1.html](https://tonyabasy.github.io/flink-sql-bootstrap/flink-compat-test-1.0.1.html)
