<div class="hero">

# Flink SQL Bootstrap

<p class="tagline">可能你的 Flink SQL 生产级部署仅需要：一个 3M 的 JAR</p>

<div class="badges">
  <span class="badge badge-green">Flink 1.20+</span>
  <span class="badge badge-blue">Apache 2.0</span>
  <span class="badge badge-orange">Java 11+</span>
</div>

</div>

## 为什么不用原生 Flink SQL CLI？

原生 Flink SQL 很强大，但在生产环境中有三个痛点：

| 痛点 | 原生 Flink SQL | Flink SQL Bootstrap |
|:-----|:---------------|:--------------------|
| **Catalog 复用** | DDL 与作业强耦合，每次提交都需重建表 | Catalog 快照 JSON — 表、视图、UDF 提前注册，运行时零 DDL |
| **多语句脚本** | 原生仅支持单语句提交 | 完整多语句支持 — DDL 立即执行，DML 统一编译为一条 Pipeline |
| **资源调优** | 仅支持 TM/JM 级别的粗粒度配置 | 算子级注入 — CPU、堆内存、托管内存、并行度、算子链策略 |

## 快速开始

```
# 前置要求: Java 11+, Flink 1.20+
# 1. 拷贝 flink-sql-gateway jar:
cp $FLINK_HOME/opt/flink-sql-gateway-*.jar $FLINK_HOME/lib

# 2. 从 GitHub Releases 下载最新 JAR:
#    https://github.com/tonyabasy/flink-sql-bootstrap/releases

# 3. 运行:
$FLINK_HOME/bin/flink run \
    --target local \
    flink-sql-bootstrap-${version}.jar \
    --script-file my_job.sql
```

[完整使用指南 →](guide.md)

---

<div class="section-title">核心能力</div>

<div class="features">

<div class="feature">
<h4>📦 Catalog 快照</h4>
<p>将表、视图、UDF 序列化为 JSON 文件。SQL 脚本中无需 DDL。</p>
</div>

<div class="feature">
<h4>📝 多语句 SQL</h4>
<p>在一个 .sql 文件中编写 DDL、DML、SET、CALL。自动拆解、校验和编排。</p>
</div>

<div class="feature">
<h4>⚙️ 算子级资源调优</h4>
<p>按算子调整 CPU、内存、并行度和算子链策略。通过 --init-resource 生成模板。</p>
</div>

<div class="feature">
<h4>🌐 通用协议支持</h4>
<p>从 classpath:、file://、http(s)://、hdfs://、s3:// 加载 SQL 脚本和配置。</p>
</div>

<div class="feature">
<h4>🚀 全部署模式</h4>
<p>支持 Local、YARN（App/Session）、Kubernetes（App/Session）所有部署模式。</p>
</div>

<div class="feature">
<h4>🔍 SQL 校验</h4>
<p>通过 --validate 校验 SQL 语法。错误报告精确到行号和列号。</p>
</div>

</div>

---

<div class="section-title">动态</div>

<div class="home-list">

<div class="item">
  <span class="date">2026-06-19</span>
  <a href="https://github.com/tonyabasy/flink-sql-bootstrap/releases/tag/v1.0.1">v1.0.1 发布</a>
  &mdash; 资源规格按算子职责重命名、文档站点上线
</div>

<div class="item">
  <span class="date">2026-06-16</span>
  <a href="#/blogs/02-cicd-pipeline.md">Flink 实时数仓开发实战：像后端那样 CI/CD</a>
</div>

<div class="item">
  <span class="date">2026-06-10</span>
  <a href="#/blogs/01-hive-like-flink-sql.md">Flink 实时数仓开发实战：像 Hive 那样用 Flink SQL</a>
</div>

<div class="item">
  <span class="date">2026-06-08</span>
  <a href="https://github.com/tonyabasy/flink-sql-bootstrap/releases/tag/v1.0.0">v1.0.0 发布</a>
  &mdash; 首次发布：多语句 SQL、Catalog 快照、资源调优
</div>

</div>

<div class="site-footer">
  <a href="https://github.com/tonyabasy/flink-sql-bootstrap">GitHub</a> &nbsp;·&nbsp;
  基于 <a href="https://github.com/tonyabasy/flink-sql-bootstrap/blob/main/LICENSE">Apache 2.0</a> 协议开源
</div>
