# Flink Multi-Version Compatibility Test

Verifies that the JAR built from `flink-sql-bootstrap` is compatible with multiple Flink versions (1.17 ~ 2.2) and deployment modes.

Design goal: **To add a new Flink version, append one line to `versions.yaml` — nothing else needs to change.**

---

## Why This Test Tool Is Needed

`flink-sql-bootstrap` relies on reflection to invoke Flink internal APIs (e.g. `TableEnvironmentImpl#executeInternal`) for fine-grained resource injection and SQL script execution. Between Flink 1.x and 2.x, incompatible changes exist in API signatures, class paths, and constructor parameters. A JAR that compiles against one version may throw `NoSuchMethodError` or `ClassNotFoundException` at runtime on another.

Core value of this tool:

| Scenario | Purpose |
|----------|---------|
| Upgrading Flink dependency | Verify the JAR still submits and runs on the new version |
| Modifying reflection logic | Ensure backward compatibility is not broken |
| External contributors | Quickly validate whether a PR introduces version incompatibilities |

---

## Supported Scope

| Dimension | Description |
|-----------|-------------|
| Flink versions | 1.17.2, 1.18.1, 1.19.3, 1.20.4, 2.0.2, 2.1.1, 2.2.0 |
| Deployment modes | Local, YARN Application, YARN Session, K8s Session, K8s Application |
| Test modes | Full execution, Validate, Compile, Init-Resource |

---

## File Structure

```
scripts/flink-compat-test/
├── config.yaml          # Test script config (JAR path, SQL script, timeout, modes, etc.)
├── versions.yaml        # Flink version config (version / java / download_url)
├── common.sh            # Shared utilities (download, run, result recording)
├── test-local.sh        # Local mode tests
├── test-yarn.sh         # YARN mode tests
├── test-k8s.sh          # K8s mode tests
└── gen-report.py        # HTML report generator

results/raw/             # JSON raw results (last 20 runs retained)
docs/flink-compat-test-<version>.html   # Compatibility report
```

---

## Design & Architecture

### Layered Structure

```
┌─────────────────────────────────────────────┐
│           gen-report.py                      │  ← Report generation (JSON → HTML)
├─────────────────────────────────────────────┤
│  test-{local,yarn,k8s}.sh                    │  ← Mode-specific test scripts
├─────────────────────────────────────────────┤
│  common.sh                                   │  ← Shared utilities (core logic)
├─────────────────────────────────────────────┤
│  versions.yaml + config.yaml                 │  ← Config layer (versions + runtime params)
└─────────────────────────────────────────────┘
```

**Why split scripts by deployment mode?**

- Local mode has zero dependencies, suitable for daily development and CI
- YARN mode requires Docker Compose to spin up a pseudo-cluster; K8s mode requires kind — environment setup logic differs significantly
- Splitting keeps each script single-responsibility, making failures faster to locate

### Core Flow

```
versions.yaml ──→ common.sh reads version list
                      │
                      ▼
              Check local cache ~/.flink-dist/
              Miss → download → extract
                      │
                      ▼
              For each version × each mode:
              flink run --target <mode> <JAR> --script-file <SQL>
                      │
                      ▼
              Detect job output (+I[ means success)
              Timeout (default 30s) without detection → FAIL
                      │
                      ▼
              Write result to results/raw/<version>_<mode>.json
              Retain last 20 runs
                      │
                      ▼
              gen-report.py aggregates → docs/flink-compat-test-<version>.html
```

### Definition of Pass / Fail

A version × mode combination is marked **PASS** if and only if:

1. The `flink run` command submits the job successfully (no submission-time exception)
2. The running job produces expected sink output (detecting the `+I[` prefix, confirming the datagen → print pipeline works)
3. The above is verified within `job_timeout` seconds

Marked **FAIL** when:

- Submission-time exception (`ClassNotFoundException`, `NoSuchMethodError`, etc. API incompatibility)
- Runtime exception (OOM, connection failure, etc.)
- Timeout without detected output (job stuck or logic error)

### Result Persistence Design

Each version × mode result is stored as JSON:

```json
{
  "history": [
    { "timestamp": "2026-06-03T12:00:00Z", "status": "PASS", "duration_s": 15, "error": "", "cmd": "..." }
  ],
  "latest": { "timestamp": "2026-06-03T12:00:00Z", "status": "PASS", "duration_s": 15, "error": "", "cmd": "..." }
}
```

- `history` retains the last 20 runs for trend analysis
- `latest` always points to the most recent result, read directly during report generation
- Append-only instead of overwrite, preventing data loss from concurrency or interruption

---

## Quick Start

### Prerequisites

- Java 11+
- Python 3
- Maven

### 1. Build the JAR

```bash
mvn clean package -DskipTests
```

### 2. Run Tests

```bash
# Local mode (no external cluster needed, recommended for daily validation)
./scripts/flink-compat-test/test-local.sh
./scripts/flink-compat-test/test-local.sh --version 2.2.0

# YARN mode (requires Docker)
./scripts/flink-compat-test/test-yarn.sh
./scripts/flink-compat-test/test-yarn.sh --mode yarn-application

# K8s mode (requires kind + kubectl + Docker)
./scripts/flink-compat-test/test-k8s.sh
./scripts/flink-compat-test/test-k8s.sh --mode kubernetes-session
```

### 3. Generate Report

```bash
python3 scripts/flink-compat-test/gen-report.py
```

Output: `docs/flink-compat-test-<pom-version>.html`

The report includes:
- Compatibility matrix (version × deployment mode)
- Failure details with error classification (API incompatibility, environment issue, resource issue, etc.)
- Test SQL and execution commands

---

## Configuration

### Adding a New Flink Version

Edit `versions.yaml` and append one line:

```yaml
  - version: "2.3.0"
    java: "17"
    download_url: "https://archive.apache.org/dist/flink/flink-2.3.0/flink-2.3.0-bin-scala_2.12.tgz"
```

`common.sh` will automatically parse and include it in the test scope.

### Adjusting Test Parameters

Edit `config.yaml`:

| Config | Description |
|--------|-------------|
| `app_jar` | Path to the JAR under test (relative to project root or absolute) |
| `test_script` | Test SQL script, default `classpath:example-word-count.sql` |
| `job_timeout` | Seconds to wait for job output, default 30 |
| `run_mode` | Empty string for full execution; `validate` / `compile` / `init-resource` for dry-run |
| `modes` | List of enabled deployment modes |
| `dependencies` | Additional dependency JARs (e.g. UDFs) |

---

## CI Integration

Recommended: run Local mode tests automatically on push / PR via GitHub Actions:

- **Cache**: Flink distributions (`~/.flink-dist`) + Maven dependencies to avoid repeated downloads. Cache key is bound to `versions.yaml` hash; cache auto-invalidates when a new version is added
- **Timeout**: recommend 30 minutes total
- **Result persistence**: commit `results/` and reports back to the repo, or output as PR comments
- **Triggers**: push to main, PR to main, manual trigger (workflow_dispatch)

---

## Roadmap

1. GitHub Actions automated CI for Local mode
2. YARN mode validation (Docker Compose)
3. K8s mode validation (kind)
4. Report enhancements (historical trends, delta analysis)
