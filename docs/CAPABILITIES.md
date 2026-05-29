<p align="center">
  <a href="CAPABILITIES.md">English</a> |
  <a href="CAPABILITIES_CN.md">中文</a>
</p>

### What It Is

1. **Flink SQL Bootstrap is a production-grade enhanced launcher for Flink SQL jobs**, providing production-level enhancements for Flink SQL job submission and configuration.
2. **Supports custom Catalog (Table, View, UDF) snapshots**, providing foundational support for real-time data warehouses in the form of reusable offline metadata, and establishing a standardized real-time data warehouse environment.
3. **Supports Multi-Statement SQL Script deployment across all modes and resource environments**, covering Application Mode, Session Mode, and Local Mode, as well as YARN, Kubernetes, and Standalone resource environments.
4. **Supports fine-grained resource tuning for Flink SQL**, filling the gap left by the official support for only coarse-grained TM/JM-level configuration.

### What It Is Not

1. **Not a Flink SQL Gateway.** Flink 2.x provides Multi-Statement SQL Script capabilities through SQL Gateway, but this approach has certain limitations and does not align with the job submission habits of most users. We will not replicate that.
2. **Not a utility component** (in the way that a library like `commons-lang3` is). Flink SQL Bootstrap is a **Flink SQL Application Template** — a new paradigm for Flink SQL job submission.
3. **Not a commercial project.** This project's original intent is to provide Flink users with a production-grade approach and methodology for Flink SQL. It is not intended for profit.

### Boundaries

1. **Embraces Flink's open-source capabilities** — no customized modifications are made to the Flink engine, Planner, or any other official standards.
2. **Provides no special SQL dialects** and introduces no semantic changes — users get exactly the same results as submitting jobs directly with Flink.
3. **Does not alter any user Flink configuration** — all user-defined Flink configuration options are written into Flink Configuration as-is.

### When It Ends

1. When the Flink project officially provides comparable capabilities.
2. When this approach no longer delivers significant value to users and a better alternative has emerged in the industry.