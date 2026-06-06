# Flink 多版本兼容性测试

验证 `flink-sql-bootstrap` 构建的 JAR 在 Flink 1.17 ~ 2.2 多个版本及不同部署模式下的兼容性。

设计目标：**新增一个 Flink 版本，只需在 `versions.yaml` 追加一行配置，其余无需改动。**

---

## 为什么需要这个测试工具

`flink-sql-bootstrap` 通过反射调用 Flink 内部 API（如 `TableEnvironmentImpl#executeInternal`）实现细粒度资源注入和 SQL 脚本执行。Flink 1.x 到 2.x 在 API 签名、类路径、构造器参数上存在不兼容变更，一个在某版本编译通过的 JAR 可能在另一版本运行时抛出 `NoSuchMethodError` 或 `ClassNotFoundException`。

本工具的核心价值：

| 场景 | 作用 |
|------|------|
| 升级 Flink 依赖 | 验证 JAR 在新版本是否仍能正常提交和运行 |
| 修改反射逻辑 | 确保没有破坏对旧版本的兼容 |
| 外部贡献者 | 快速验证 PR 是否引入版本不兼容问题 |

---

## 支持范围

| 维度 | 说明 |
|------|------|
| Flink 版本 | 1.20.4, 2.0.2, 2.1.1, 2.2.0 |
| 部署模式 | Local、YARN Application、YARN Session、K8s Session、K8s Application |
| 测试方式 | 完整执行、Validate、Compile、Init-Resource 四种运行模式 |

---

## 文件结构

```
scripts/flink-compat-test/
├── config.yaml          # 测试脚本配置（JAR 路径、SQL 脚本、超时、模式等）
├── versions.yaml        # Flink 版本配置（version / java / download_url）
├── common.sh            # 共享工具库（下载、运行、结果记录）
├── test-local.sh        # Local 模式测试
├── test-yarn.sh         # YARN 模式测试
├── test-k8s.sh          # K8s 模式测试
└── gen-report.py        # HTML 报告生成器

results/raw/             # JSON 原始结果（保留最近 20 条历史）
docs/flink-compat-test-<version>.html   # 兼容性报告
```

---

## 设计与架构

### 分层结构

```
┌─────────────────────────────────────────────┐
│           gen-report.py                      │  ← 报告生成（读取 JSON → HTML）
├─────────────────────────────────────────────┤
│  test-{local,yarn,k8s}.sh                    │  ← 模式测试脚本（按部署模式拆分）
├─────────────────────────────────────────────┤
│  common.sh                                   │  ← 共享工具库（核心逻辑）
├─────────────────────────────────────────────┤
│  versions.yaml + config.yaml                 │  ← 配置层（版本 + 运行参数）
└─────────────────────────────────────────────┘
```

**为什么按部署模式拆分脚本？**

- Local 模式零依赖，适合日常开发和 CI
- YARN 模式需要 Docker Compose 启动伪集群，K8s 模式需要 kind，环境准备逻辑差异大
- 拆分后每个脚本职责单一，失败时定位更快

### 核心流程

```
versions.yaml ──→ common.sh 读取版本列表
                      │
                      ▼
              检查本地缓存 ~/.flink-dist/
              未命中 → 下载 → 解压
                      │
                      ▼
              对每个版本 × 每个模式：
              flink run --target <mode> <JAR> --script-file <SQL>
                      │
                      ▼
              检测 Job 输出（+I[ 表示成功）
              超时（默认 30s）未检测到 → FAIL
                      │
                      ▼
              结果写入 results/raw/<version>_<mode>.json
              保留最近 20 条历史
                      │
                      ▼
              gen-report.py 聚合 → docs/flink-compat-test-<version>.html
```

### 测试通过的定义

一个版本 × 模式的组合判定为 **PASS**，当且仅当：

1. `flink run` 命令成功提交 Job（无提交期异常）
2. Job 运行时产生预期的 Sink 输出（检测到 `+I[` 前缀，说明 datagen → print 链路跑通）
3. 在 `job_timeout` 秒内完成上述验证

判定为 **FAIL** 的情况包括：
- 提交期异常（`ClassNotFoundException`、`NoSuchMethodError` 等 API 不兼容）
- 运行时异常（OOM、连接失败等）
- 超时未检测到输出（Job 卡住或逻辑错误）

### 结果持久化设计

每个版本 × 模式的结果以 JSON 存储，结构如下：

```json
{
  "history": [
    { "timestamp": "2026-06-03T12:00:00Z", "status": "PASS", "duration_s": 15, "error": "", "cmd": "..." }
  ],
  "latest": { "timestamp": "2026-06-03T12:00:00Z", "status": "PASS", "duration_s": 15, "error": "", "cmd": "..." }
}
```

- `history` 保留最近 20 条，支持趋势分析
- `latest` 始终指向最近一次结果，报告生成时直接读取
- 追加写而非覆盖，避免并发或中断导致数据丢失

---

## 快速开始

### 前置条件

- Java 11+
- Python 3
- Maven

### 1. 构建 JAR

```bash
mvn clean package -DskipTests
```

### 2. 运行测试

```bash
# Local 模式（无需外部集群，推荐日常验证）
./scripts/flink-compat-test/test-local.sh
./scripts/flink-compat-test/test-local.sh --version 2.2.0

# YARN 模式（需要 Docker）
./scripts/flink-compat-test/test-yarn.sh
./scripts/flink-compat-test/test-yarn.sh --mode yarn-application

# K8s 模式（需要 kind + kubectl + Docker）
./scripts/flink-compat-test/test-k8s.sh
./scripts/flink-compat-test/test-k8s.sh --mode kubernetes-session
```

### 3. 生成报告

```bash
python3 scripts/flink-compat-test/gen-report.py
```

输出：`docs/flink-compat-test-<pom-version>.html`

报告包含：
- 兼容性矩阵表（版本 × 部署模式）
- 失败详情与错误分类（API 不兼容、环境问题、资源问题等）
- 测试 SQL 与执行命令

---

## 配置说明

### 新增 Flink 版本

编辑 `versions.yaml`，追加一行即可：

```yaml
  - version: "2.3.0"
    java: "17"
    download_url: "https://archive.apache.org/dist/flink/flink-2.3.0/flink-2.3.0-bin-scala_2.12.tgz"
```

`common.sh` 会自动解析并纳入测试范围。

### 调整测试参数

编辑 `config.yaml`：

| 配置项 | 说明 |
|--------|------|
| `app_jar` | 被测 JAR 路径（支持相对项目根目录或绝对路径） |
| `test_script` | 测试 SQL 脚本，默认 `classpath:example-word-count.sql` |
| `job_timeout` | 等待 Job 输出超时秒数，默认 30 |
| `run_mode` | 空字符串为完整执行；`validate` / `compile` / `init-resource` 为干运行 |
| `modes` | 启用的部署模式列表 |
| `dependencies` | 额外依赖 JAR（如 UDF） |

---

## CI 集成

推荐通过 GitHub Actions 在 push / PR 时自动运行 Local 模式测试：

- **缓存**：Flink 发行版（`~/.flink-dist`）+ Maven 依赖，避免重复下载。缓存 key 绑定 `versions.yaml` hash，新增版本时自动失效
- **超时**：建议总上限 30 分钟
- **结果持久化**：可将 `results/` 和报告提交回仓库，或作为 PR comment 输出
- **触发**：push 到 main、PR 到 main、手动触发（workflow_dispatch）

---

## 路线图

1. GitHub Actions Local 模式自动化 CI
2. YARN 模式验证（Docker Compose）
3. K8s 模式验证（kind）
4. 报告增强（历史趋势、增量分析）
