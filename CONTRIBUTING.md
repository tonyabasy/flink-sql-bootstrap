# Contributing to Flink SQL Bootstrap

Thank you for your interest in contributing! We welcome contributions of all kinds — bug reports, feature requests, documentation improvements, and code changes.

## Table of Contents

- [Quick Start for Contributors](#quick-start-for-contributors)
- [Development Setup](#development-setup)
- [Building and Testing](#building-and-testing)
- [Code Style](#code-style)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)

## Quick Start for Contributors

```bash
# Fork and clone the repository
git clone https://github.com/your-username/flink-sql-bootstrap.git
cd flink-sql-bootstrap

# Build the project
mvn package -DskipTests

# Run all tests
mvn test
```

## Development Setup

### Prerequisites

| Dependency | Version |
|------------|---------|
| Java | 11+ (JDK, not JRE) |
| Maven | 3.6+ |
| Flink | 1.20+ (optional, for integration testing) |

### IDE Setup

**IntelliJ IDEA** (recommended):
1. Open the project directory
2. Ensure Lombok plugin is installed and annotation processing is enabled
3. Import the code style from `style/` if available

**VS Code**:
- Install the Extension Pack for Java and Lombok Annotations Support

## Building and Testing

### Build the fat JAR

```bash
mvn package -DskipTests
```

Output is at `target/flink-sql-bootstrap-1.0-SNAPSHOT.jar`.

### Run all tests

```bash
mvn test
```

45 unit tests covering the core execution engine, resource injection, and utility classes.

### Run style check

```bash
mvn spotless:check
```

To automatically fix style issues:

```bash
mvn spotless:apply
```

### Test with a real Flink cluster

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    target/flink-sql-bootstrap-1.0-SNAPSHOT.jar \
    --script-file classpath:example-word-count.sql
```

## Code Style

This project uses [Spotless](https://github.com/diffplug/spotless) with Google Java Format to enforce a consistent code style. The check runs automatically during `mvn verify` and in CI.

Key conventions:

- **Imports**: grouped in the order `#` → `com.lanting` → `org.apache.flink` → `java` → `javax` → `org` → `com` → other
- **License header**: every Java file must include the Apache 2.0 license header (template in `style/license_header`)
- **No unused imports** — Spotless removes them automatically
- **No trailing whitespace**

The CI build will fail if code style is not compliant. Run `mvn spotless:apply` before committing to fix most issues automatically.

## Pull Request Process

1. **Create an issue first** — for significant changes, open an issue to discuss the approach before writing code
2. **Keep PRs focused** — one PR should address one concern. If you find unrelated issues, file them separately
3. **Write tests** — new features should include tests; bug fixes should include a test that reproduces the bug
4. **Run tests locally** — ensure `mvn verify` passes before pushing
5. **Update documentation** — if your change affects the CLI, configuration, or behavior, update the relevant docs
6. **Use conventional commits** — follow the [Conventional Commits](https://www.conventionalcommits.org/) format for commit messages (e.g., `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`)
7. **Wait for CI** — the GitHub Actions build must pass before merging

### PR Checklist

Before submitting, check that your PR:

- [ ] Passes `mvn verify`
- [ ] Includes tests for new or changed functionality
- [ ] Updates documentation (README, CHANGELOG, or API docs as needed)
- [ ] Follows the existing code style
- [ ] Has no merge conflicts

## Issue Reporting

### Bug Reports

When filing a bug report, include:

- A clear description of the problem
- Steps to reproduce (include SQL script, CLI flags, and Flink version)
- Expected vs. actual behavior
- Environment details (OS, Java version, Flink version)

### Feature Requests

Describe the problem you want to solve, not just the feature you want. This helps us design the right solution.

---

Thank you for contributing!