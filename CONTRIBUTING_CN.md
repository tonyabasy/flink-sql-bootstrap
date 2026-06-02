# 参与 Flink SQL Bootstrap 贡献

欢迎各种形式的贡献 —— Bug 报告、功能建议、文档改进和代码修改。

## 目录

- [贡献者快速开始](#贡献者快速开始)
- [开发环境配置](#开发环境配置)
- [构建与测试](#构建与测试)
- [代码风格](#代码风格)
- [Pull Request 流程](#pull-request-流程)
- [提交 Issue](#提交-issue)

## 贡献者快速开始

```bash
# Fork 并克隆仓库
git clone https://github.com/your-username/flink-sql-bootstrap.git
cd flink-sql-bootstrap

# 构建项目
mvn package -DskipTests

# 运行所有测试
mvn test
```

## 开发环境配置

### 环境要求

| 依赖 | 版本 |
|------|------|
| Java | 11+（需要 JDK，JRE 不够） |
| Maven | 3.6+ |
| Flink | 1.20+（可选，集成测试时需要） |

### IDE 配置

**IntelliJ IDEA**（推荐）：
1. 打开项目目录
2. 安装 Lombok 插件，启用注解处理（Annotation Processing）
3. 如果 `style/` 目录下有代码风格配置，可导入

**VS Code**：
- 安装 Java 扩展包和 Lombok 注解支持

## 构建与测试

### 构建 fat JAR

```bash
mvn package -DskipTests
```

输出文件位于 `target/flink-sql-bootstrap-1.0-SNAPSHOT.jar`。

### 运行测试

```bash
mvn test
```

45 个单元测试覆盖核心执行引擎、资源注入和工具类。

### 运行代码风格检查

```bash
mvn spotless:check
```

自动修复格式问题：

```bash
mvn spotless:apply
```

### 在真实 Flink 集群上测试

```bash
$FLINK_HOME/bin/flink run \
    --target local \
    target/flink-sql-bootstrap-1.0-SNAPSHOT.jar \
    --script-file classpath:example-word-count.sql
```

## 代码风格

本项目使用 [Spotless](https://github.com/diffplug/spotless) 配合 Google Java Format 强制执行一致的代码风格。`mvn verify` 和 CI 中会自动运行检查。

关键约定：

- **导入顺序**：`#` → `com.lanting` → `org.apache.flink` → `java` → `javax` → `org` → `com` → 其他
- **License 头**：每个 Java 文件必须包含 Apache 2.0 License header（模板在 `style/license_header`）
- **无未使用的导入** —— Spotless 会自动删除
- **无行尾空格**

如果代码风格不合规，CI 构建会失败。提交前运行 `mvn spotless:apply` 可自动修复大部分问题。

## Pull Request 流程

1. **先建 Issue** —— 重大变更请先建 Issue 讨论方案，再开始写代码
2. **PR 保持专注** —— 一个 PR 只处理一个问题。发现不相关的缺陷请另建 Issue
3. **编写测试** —— 新功能要有对应的测试；Bug 修复要有复现该 Bug 的测试
4. **本地跑通测试** —— 确保 `mvn verify` 通过后再推送
5. **更新文档** —— 如果你的变更影响 CLI、配置或行为，请更新相应文档
6. **使用 Conventional Commits** —— 提交信息遵循 [Conventional Commits](https://www.conventionalcommits.org/) 格式（如 `feat:`、`fix:`、`docs:`、`test:`、`refactor:`、`chore:`）
7. **等待 CI 通过** —— GitHub Actions 构建必须通过后才能合并

### PR 检查清单

提交前确认你的 PR：

- [ ] `mvn verify` 通过
- [ ] 包含新功能或变更的测试
- [ ] 更新了相关文档（README、CHANGELOG、API 文档等）
- [ ] 遵循现有代码风格
- [ ] 无合并冲突

## 提交 Issue

### Bug 报告

提交 Bug 时请包含：

- 问题的清晰描述
- 复现步骤（包括 SQL 脚本、CLI 参数和 Flink 版本）
- 预期行为和实际行为
- 环境信息（操作系统、Java 版本、Flink 版本）

### 功能建议

描述你想要解决的问题，而不只是你想要的功能。这有助于我们设计更合适的方案。

---

感谢你的贡献！