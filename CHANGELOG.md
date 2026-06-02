# Changelog

## [0.1.0] - 2026-06-02

### Features

- 初始化 Flink SQL Bootstrap 项目，支持 Multi-Statement SQL Script 执行
- 重构 DAG 打印机和 SQL 编译链路，支持编译时验证
- 兼容 Flink 1.20.x Application Mode，新增示例文件
- 新增 `--validate` 选项，支持 SQL 语法校验而不提交到集群
- SQL 解析错误附带行号信息，便于快速定位语法问题
- 新增 `SqlError` 异常类，错误消息包含行列号
- 新增 `--init-resource` 选项，生成算子级资源调优模板
- Catalog 快照支持表、视图、UDF 预注册
- 算子级资源注入（CPU/Heap/Managed/Parallelism/ChainStrategy）

### Bug Fixes

- 修复注释与代码不一致的问题
- 移除调试输出和 FIXME 注释
- 添加路径遍历防护
- 修复 Printer 在 NOT_READY 状态下的忙等问题（指数退避）

### Refactor

- 重组异常类，统一继承 `SqlScriptException`
- 重组测试资源目录结构
- 引入 `@Experimental` 注解标记实验性 API

### Tests

- 新增 StreamingScriptExecutor 核心测试（8 用例）
- 新增 injectResourceSpec + generateResultSpec 测试（11 用例）
- 总计 45 个单元测试覆盖核心逻辑

### Documentation

- 更新项目描述，强调三大核心能力
- 新增能力边界文档（docs/CAPABILITIES.md）
- 新增中英文版能力边界文档
- 添加 CONTEXT.md 领域术语表
- 优化 CLI help 并统一术语
- 完善 README，补充 CLI 选项、编译模式、示例输出
- 根据实际执行结果修正 README 中的示例输出
- 新增 docs/agents/ 目录，提供 AI Agent 协作指南

### Build & CI

- 添加 Apache 2.0 License 头，完善开源合规文件
- 配置 Spotless 代码风格检查
- 配置 Maven shade plugin 打包 fat JAR
- 配置 GitHub Actions CI（Java 11/17/21 矩阵构建）

### Other

- 清理重复的 SQL 示例文件
- 忽略 AI 开发工具配置文件