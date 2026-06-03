# 更新日志

本项目的所有显著变更都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
并且本项目遵循 [语义化版本控制](https://semver.org/lang/zh-CN/spec/v2.0.0.html)。

## [Unreleased]

### Added（新增）

- **Multi-Statement SQL Script 执行引擎** —— 支持在单个 `.sql` 文件中混合编写 `CREATE TABLE`、`SET`、`INSERT`、`CALL` 等多条语句，按语义自动切分、校验并编排执行顺序，DML 语句延迟到编译阶段统一提交
- **Catalog 快照预注册** —— 支持通过 JSON 文件预先注册表、视图和 UDF，作业启动时 Catalog 已完全就绪，SQL 脚本中无需再写 DDL，支持 `classpath:`、`file://`、`http(s)://`、`hdfs://` 多协议加载
- **算子级细粒度资源注入** —— 在 Transformation DAG 层面按算子 UID 或名称注入并行度、CPU、堆内存、托管内存、Off-Heap 内存、外部资源及链策略（Chain Strategy），相同资源配置的算子自动归组到同一 SlotSharingGroup
- **SQL 语法校验（`--validate`）** —— 本地快速校验 SQL 语法，无需提交到 Flink 集群；解析错误精确到行号和列号，便于快速迭代
- **SQL 编译与执行计划输出（`--compile`）** —— 解析、校验并编译 SQL，输出 `InternalPlan` JSON 执行计划，不实际提交作业，用于预览和调试
- **资源模板自动生成（`--init-resource`）** —— 基于当前 SQL 脚本自动提取 Transformation DAG 结构，生成算子级资源配置 JSON 模板，用户修改数值后即可注入
- **Flink 1.20.x / 2.x 双版本兼容** —— 通过 `ApplicationOperationExecutor` 绕过 SPI 兼容性检查，通过 `UriSafeSessionContext` 修复 `URI→URL` 类型转换导致的 `ArrayStoreException`，支持 Application Mode 下正常启动作业
- **确定性算子 UID 生成** —— 强制开启 `TABLE_EXEC_UID_GENERATION = ALWAYS`，确保每个 Transformation 拥有稳定 UID，作为资源配置 JSON 的精确匹配键
- **SQL 执行结果格式化打印** —— 兼容 Flink 1.20.x/2.x 的 `TableauStyle` 结果表渲染，带指数退避的 `RowDataIterator` 轮询结果
- **异常体系** —— 定义 `SqlValidateException`、`SqlCompileException`、`SqlParsePosException`，SQL 解析错误附带源码行列位置信息
- **实验性 API 标记** —— 引入 `@Experimental` 注解，用于标记 DAG 打印器等孵化中的 API
- **DAG 拓扑可视化（实验性）** —— ASCII 艺术渲染 Transformation DAG，支持双 Source Join、Union、多路聚合等典型拓扑结构的控制台打印

### Build & CI（构建与持续集成）

- 基于 Maven 构建，配置 Shade Plugin 打包 fat JAR，主类为 `SqlEntryPoint`
- 配置 Spotless 代码格式化与 Apache 2.0 License Header 自动注入
- 配置 GitHub Actions CI，Java 11 / 17 / 21 矩阵构建
- 45 个单元测试覆盖核心执行器、资源注入、入口安全、资源规格签名等逻辑

### Documentation（文档）

- 提供完整的中英文 README，含快速开始、CLI 选项说明、配置示例
- 提供能力边界文档（[CAPABILITIES_CN.md](docs/CAPABILITIES_CN.md)）
- 提供领域术语表（[CONTEXT_CN.md](CONTEXT_CN.md)）
- 提供 SQL 示例（[`example-word-count.sql`](src/main/resources/example-word-count.sql)、[`example-word-count-advanced.sql`](src/main/resources/example-word-count-advanced.sql)）
- 提供 Catalog 快照示例（[`example-catalog.json`](src/main/resources/example-catalog.json)）
- 提供算子资源配置示例（[`example-resource.json`](src/main/resources/example-resource.json)）
- 提供 UDF 示例 JAR（`example-udf-reverse.jar`、`example-udf-substring.jar`）
- AI Agent 协作指南 (`docs/agents/`)
- Flink 多版本兼容性测试套件 (`scripts/flink-cmp-test/`) —— 自动化测试 Flink 1.17 ~ 2.2 在 Local、YARN、Kubernetes 部署模式下的兼容性，生成 HTML 兼容性报告 (`docs/flink-compat-test-<version>.html`)，包含通过/失败矩阵和错误分类

- **YARN** 和 **Kubernetes** 部署模式尚未测试（报告中标记为 NT）。
- 完整兼容性报告：[docs/flink-compat-test-1.0-SNAPSHOT.html](docs/flink-compat-test-1.0-SNAPSHOT.html)
- 提供领域术语表（[CONTEXT.md](CONTEXT.md)）
- 提供 SQL 示例（[`example-word-count.sql`](src/main/resources/example-word-count.sql)、[`example-word-count-advanced.sql`](src/main/resources/example-word-count-advanced.sql)）
- 提供 Catalog 快照示例（[`example-catalog.json`](src/main/resources/example-catalog.json)）
- 提供算子资源配置示例（[`example-resource.json`](src/main/resources/example-resource.json)）
- 提供 UDF 示例 JAR（`example-udf-reverse.jar`、`example-udf-substring.jar`）
