# Flink SQL Bootstrap

A bootstrap launcher for Flink SQL jobs. It runs a Flink SQL application from a user-provided SQL script, with support for Catalog snapshot restoration and fine-grained per-operator resource management.

## Language

**SQL Script**:
A text input provided by the user containing multiple statements. It may include SET / RESET / CALL / DDL statements, but at most one DML statement or one STATEMENT SET. It defines a complete Flink SQL application.
_Avoid_: SQL file, job definition

**Catalog Snapshot**:
A JSON-formatted snapshot of Flink Catalog state, containing complete metadata definitions for tables, views, and UDFs. Used to restore the Catalog offline at job startup without connecting to an external metadata service.
_Avoid_: Catalog config, metadata file

**Operator Resource Spec**:
A JSON configuration file that declares fine-grained resource parameters (parallelism, CPU, memory, chain strategy) per operator UID or name, injected into the Transformation DAG before job execution.
_Avoid_: resource config, operator config

**SQL Script Executor**:
The component that manages the full execution lifecycle of an SQL script — multi-statement SQL script splitting, SQL parsing, SQL validation, SQL compilation, translation into a Transformation DAG, operator resource spec injection, and submission for execution.
_Avoid_: compile pipeline, execution flow

**Operator Matching**:
The process of mapping each rule in the operator resource spec to the corresponding operator in the Transformation DAG — exact match by UID first, with operator name matching as fallback.
_Avoid_: rule matching, resource config mapping

**UID Generation**:
The mechanism by which Flink automatically assigns a unique identifier to each operator. This project forces the ALWAYS mode to ensure that operator UIDs remain consistent across compilations of the same SQL script, providing a stable foundation for precise operator resource spec matching.
_Avoid_: operator ID, transformation ID

**Compile Mode**:
Executes only the parsing, validation, and compilation phases of the SQL script, outputting the JSON representation of the `InternalPlan` without submitting the job for execution.
_Avoid_: validation mode, dry run

**Init Resource Mode**:
Does not submit the job. Compiles the user-provided SQL script to obtain the Transformation DAG, then generates an operator resource spec JSON template for each operator by UID and name, for the user to fill in actual resource parameters.
_Avoid_: generate template, export config
