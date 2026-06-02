<p align="center">
  <a href="CAPABILITIES.md">English</a> |
  <a href="CAPABILITIES_CN.md">中文</a>
</p>

### 是什么
1. **Flink SQL Bootstrap 是一个生产级的 Flink SQL 作业增强启动器**，为 Flink SQL 作业的提交和配置提供生产级增强。
2. **支持自定义 Catalog（Table、View、UDF）快照**，以离线数仓可复用元数据的方式为实时数仓提供底层能力支撑，构建标准化的实时数仓环境。
3. **支持 Multi-Statement SQL Script 在所有模式、所有资源环境下进行部署**，覆盖 Application Mode、Session Mode、Local Mode 以及 YARN、Kubernetes、Standalone 等资源环境。
4. **支持 Flink SQL 细粒度资源调优**，填补官方仅支持 TM/JM 级别粗粒度配置的空白。

### 不是什么

1. **不是 Flink SQL Gateway。** Flink 2.x 通过 SQL Gateway 提供了 Multi-Statement SQL Script 的能力，但这种方式有一定局限性，且不符合大部分用户提交 Flink Job 的习惯，我们不会做这些。
2. **不是一个工具组件**（类似于 `commons-lang3` 工具包那样）。Flink SQL Bootstrap 是一个 **Flink SQL Application Template**，给 Flink SQL Job 的提交提供一种新思路。

### 边界是什么

1. **拥抱 Flink 开源能力**，不对 Flink 引擎、Planner 等官方标准做任何定制化的修改。
2. **不提供任何特殊的 SQL 方言**，不进行任何语义上的变化，与用户直接使用 Flink 提交任务得到一模一样的结果。
3. **不变动用户的 Flink 配置项**，用户所有 Flink 配置如实地写入进 Flink Configuration。

### 什么时候终结

1. Flink 官方提供了类似的能力。
2. 这种方式对于用户已经没有太大的价值，业界已经有更好的替代方式。