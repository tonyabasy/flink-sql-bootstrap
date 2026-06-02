# Contributing

Thanks for your contribution! Here's a quick-start guide.

## Quick Start

```bash
git clone https://github.com/your-username/flink-sql-bootstrap.git
cd flink-sql-bootstrap
mvn verify          # build + test + code style check
```

Requirements: Java 11+, Maven 3.6+.

## Pull Requests

1. **Open an Issue first** for major changes; minor fixes (typos, docs) can go straight to PR
2. Branch from `main`: `feat/xxx`, `fix/xxx`, `docs/xxx`
3. One PR per concern
4. New features / bug fixes must include tests
5. Ensure `mvn verify` passes before pushing
6. Follow [Conventional Commits](https://www.conventionalcommits.org/):

   ```text
   feat: add --dry-run option
   fix: resolve URI conversion bug
   docs: update Kubernetes example
   ```
7. Update README / CHANGELOG if applicable

### PR Checklist

- [ ] `mvn verify` passes
- [ ] Includes tests
- [ ] Documentation updated
- [ ] No merge conflicts

## Code Style

This project uses [Spotless](https://github.com/diffplug/spotless) with Google Java Format. Auto-fix before committing:

```bash
mvn spotless:apply
```

Key rule: every Java file must include the Apache 2.0 License Header (template in `style/license_header`).

## Issues

- **Bug**: use the `bug` label, provide reproduction steps, SQL script, CLI args, and environment info
- **Feature request**: use the `enhancement` label, describe the problem to solve rather than just the feature
