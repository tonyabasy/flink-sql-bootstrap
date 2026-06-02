# Flink SQL 启动模板

Flink SQL 作业的启动模板，根据用户提供的 SQL 脚本运行 Flink SQL App，并附加 Catalog 快照恢复和细粒度算子资源管理能力。

## Language

**SQL 脚本（SQL Script）**：
用户提供的包含多条语句的文本输入，可包含 SET / RESET / CALL / DDL 语句，但至多包含一条 DML 或一条 STATEMENT SET 语句，定义了一个完整的 Flink SQL 应用。
_避免使用_：SQL 文件、作业定义

**Catalog 快照（Catalog Snapshot）**：
一个 JSON 格式的 Flink Catalog 状态快照，包含表、视图和 UDF 的完整元数据定义，用于在作业启动时离线恢复 Catalog 而无需连接外部元数据中心。
_避免使用_：Catalog 配置、元数据文件

**算子资源规格（Operator Resource Spec）**：
一个 JSON 配置文件，按算子 UID 或名称声明细粒度资源参数（并行度、CPU、内存、Chain 策略），在作业执行前注入到 Transformation DAG 中。
_避免使用_：资源配置、算子配置

**SQL Script 执行器（SQL Script Executor）**：
管理 SQL 脚本完整执行生命周期的组件，依次负责——Multi-statements SQL Script 切分、SQL 解析、SQL 验证、SQL 编译、SQL 转化为 Transformation DAG、算子资源规格注入、提交执行。
_避免使用_：编译管线、执行流程

**算子匹配（Operator Matching）**：
将算子资源规格中的每条规则匹配到 Transformation DAG 中对应算子的过程——优先按 UID 精确匹配，以算子名称匹配作为兜底。
_避免使用_：规则匹配、资源配置映射

**UID 生成（UID Generation）**：
Flink 为每个算子自动分配唯一标识符的机制。本项目强制开启 ALWAYS 模式，确保每次编译同一 SQL 脚本时算子 UID 保持一致，为算子资源规格的精确匹配提供基础。
_避免使用_：算子 ID、Transformation ID

**编译模式（Compile Mode）**：
仅执行 SQL 脚本的解析、验证和编译阶段，输出 `InternalPlan` 的 JSON 表示，不提交作业执行。
_避免使用_：校验模式、干运行

**初始化资源模式（Init Resource Mode）**：
不提交作业，根据用户提交的 SQL 脚本编译获取 Transformation DAG，为其中各算子按 UID 和名称生成算子资源规格 JSON 模板，供用户填写实际资源参数。
_避免使用_：生成模板、导出配置
