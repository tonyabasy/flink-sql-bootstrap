# 参与贡献

感谢你的贡献！以下是快速上手指南。

## 快速开始

```bash
# 或 Gitee 仓库：git@gitee.com:tonyabasy2025/flink-sql-bootstrap.git
git clone https://github.com/your-username/flink-sql-bootstrap.git
cd flink-sql-bootstrap
mvn verify          # 构建 + 测试 + 代码风格检查
```

环境要求：Java 11+、Maven 3.6+。

## 提交 Pull Request

1. **重大变更先建 Issue 讨论**，微小修复（错别字、文档修正）可直接提 PR
2. 从 `main` 切分支：`feat/xxx`、`fix/xxx`、`docs/xxx`
3. 一个 PR 只做一件事
4. 新功能/修复必须包含测试
5. 提交前确保 `mvn verify` 通过
6. 遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

   ```text
   feat: add --dry-run option
   fix: resolve URI conversion bug
   docs: update Kubernetes example
   ```
7. 更新 README / CHANGELOG（如适用）

### PR 检查清单

- [ ] `mvn verify` 通过
- [ ] 包含测试
- [ ] 更新了相关文档
- [ ] 无合并冲突

## 兼容性测试

提交修改 Flink 版本兼容性或反射逻辑的 PR 前，请先运行兼容性测试套件，确保没有引入回归：

```bash
# 先构建 JAR
mvn clean package -DskipTests

# 运行 Local 模式测试（无需外部集群）
./scripts/flink-cmp-test/test-local.sh

# 测试指定 Flink 版本
./scripts/flink-cmp-test/test-local.sh --version 2.2.0

# 生成 HTML 报告
python3 scripts/flink-cmp-test/gen-report.py
```

YARN / Kubernetes 模式测试及 CI 集成详见 [docs/flink-compat-test_CN.md](docs/flink-compat-test_CN.md)。

## 代码风格

本项目使用 [Spotless](https://github.com/diffplug/spotless) + Google Java Format。提交前自动修复：

```bash
mvn spotless:apply
```

关键约定：每个 Java 文件必须包含 Apache 2.0 License Header（模板在 `style/license_header`）。

## 提交 Issue

- **Bug**：使用 `bug` 标签，提供复现步骤、SQL 脚本、CLI 参数和环境信息
- **功能建议**：使用 `enhancement` 标签，描述要解决的问题而非仅功能本身
