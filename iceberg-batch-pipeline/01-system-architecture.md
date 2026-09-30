# System architecture and ownership

[Index](README.md) | [Next: Iceberg scan](02-iceberg-scan.md)

## Mental model

A query has several cooperating state machines. Spark controls the distributed computation. Iceberg controls the table snapshot and commit protocol. Comet has a native execution context inside a Spark task attempt. Parquet controls the physical file layout. Arrow describes in-memory arrays; it does not schedule work or publish table state.

| Component | Owns | Does not automatically own |
| --- | --- | --- |
| Spark Catalyst | SQL analysis, optimization, physical plan selection | Iceberg file contents or native decoding |
| Spark DataSource V2 | Scan/write interfaces, input partitions, writer messages | The table format's commit protocol |
| Spark AQE | Replanning around materialized query stages and observed statistics | A second Iceberg snapshot chosen inside a running native task |
| Spark scheduler | Jobs, stage dependencies, task attempts, placement, retries | Native operator algorithms |
| Iceberg Java | Catalog/table loading, snapshot/schema/spec selection, manifests, task planning, commit validation | Every executor byte must remain JVM-owned |
| Comet JVM | Eligibility, Spark physical-plan conversion, protobuf, task/native lifecycle | A separate cluster scheduler |
| Comet native and DataFusion | Local execution plans, Arrow streams, supported expressions and operators | Unrestricted replacement of Spark SQL semantics |
| Iceberg Rust | Iceberg-aware reading/deletes/adaptation and eligible file writers | Replanning Comet's scan from the catalog |
| arrow-rs | Arrays, buffers, Parquet codecs and readers/writers, FFI | Transaction visibility or retry policy |

Evidence: [Spark integration and native runtime](09-source-ledger.md#comet-runtime), [Iceberg planning](09-source-ledger.md#iceberg-planning), [DataFusion and Arrow](09-source-ledger.md#datafusion-and-arrow).

## High level component flow

```mermaid
%%{init: {"theme":"base","flowchart":{"curve":"basis","nodeSpacing":35,"rankSpacing":50},"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"15px","primaryTextColor":"#0f172a","lineColor":"#64748b"}}}%%
flowchart LR
  subgraph DRIVER["Spark driver"]
    SQL["SQL and Catalyst"]
    IP["Iceberg Java scan planning"]
    CR{"Comet eligibility"}
    S["Stages and task scheduling"]
    SQL --> IP --> CR --> S
  end
  subgraph EXEC["Spark executors"]
    N["Comet native scan and operators"]
    J["Spark and Iceberg JVM work"]
    X["Spark exchange boundaries"]
    S -- "task attempts" --> N
    S -- "fallback tasks or regions" --> J
    N --> X
    J --> X
  end
  subgraph STORE["Table storage"]
    M[("Metadata and manifests")]
    D[("Data and delete files")]
  end
  M --> IP
  D --> N
  D --> J
  classDef spark fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
  classDef iceberg fill:#fef9c3,stroke:#ca8a04,color:#713f12
  classDef native fill:#ede9fe,stroke:#7c3aed,color:#3b0764
  classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
  class SQL,CR,S,J,X spark
  class IP iceberg
  class N native
  class M,D store
```

The two executor boxes are not mutually exclusive for a whole query. A plan can contain several native regions separated by JVM operators, exchanges, or representation transitions. An unsupported scan need not imply that every downstream operator is JVM-only, and a native scan does not prove a native end-to-end query.

## End to end query sequence

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"15px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f8fafc","actorBorder":"#475569","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a"}}}%%
sequenceDiagram
  box rgb(224, 242, 254) Spark driver
    participant Q as SQL and Catalyst
    participant S as Scheduler and AQE
  end
  box rgb(254, 249, 195) Iceberg Java
    participant I as Scan planner
  end
  box rgb(237, 233, 254) Executor native region
    participant C as Comet
    participant R as Iceberg Rust and DataFusion
  end
  box rgb(241, 245, 249) Storage
    participant O as Object store or filesystem
  end
  Q->>I: Request projected and filtered scan
  I->>O: Read selected snapshot metadata and manifests
  O-->>I: Return metadata
  I-->>Q: Return planned file work and scan properties
  Q->>Q: Apply Comet conversion and transition rules
  Q->>S: Execute Spark plan and dependencies
  S->>C: Launch task with assigned scan payload
  C->>R: Build and execute native physical plan
  R->>O: Read data and applicable deletes
  O-->>R: Return file byte ranges
  R-->>C: Yield Arrow batches or shuffle output
  C-->>S: Report task output and metrics
  S->>S: Materialize dependencies and adapt later work
  S-->>Q: Complete result
```

This is a phase summary. Planning hooks can revisit a scan; AQE and runtime filters defer some work. Do not infer a single eager planning pass or a single task from the diagram.

## Distinct units of work

| Unit | Stable meaning | Common mistake |
| --- | --- | --- |
| Snapshot | A committed table state, with history and references | Treating an S3 prefix as the table state |
| Iceberg partition | A tuple produced by a partition spec | Assuming one Spark task per partition |
| Data file | A stored data artifact with manifest metadata | Assuming it contains only one row group |
| FileScanTask | A file/range plus schema, spec, residual and deletes | Treating it as only a filename |
| Spark input partition | Work assigned to a task attempt | Equating its count with Arrow batch count |
| Stage | Spark computation separated by distributed dependencies | Calling every native operator a Spark stage |
| Task attempt | One attempt to execute a scheduling partition | Assuming side effects happen once physically |
| Native execution context | A task-owned native plan and streams | Treating it as a cluster-wide DataFusion session |
| RecordBatch | Schema and equal-length arrays plus row count | Treating a batch as a Parquet page |

## Example distributed aggregate

Illustrative query: `SELECT region, SUM(amount) FROM orders WHERE event_date >= DATE '2026-01-01' GROUP BY region`.

```text
Map side: assigned Iceberg files -> native scan -> filter -> partial SUM by region -> hash shuffle write
Reduce side: fetch region partitions -> native shuffle decode -> final SUM -> result
```

The native region reduces per-row overhead; the exchange makes all contributions for one key meet. More scan tasks can improve read parallelism without changing the number of reduce partitions. Skew can leave one reduce task much slower than the others. These are different problems from Parquet decoding speed.

## Repository relationships

Comet calls Iceberg Rust directly and builds its own `IcebergScanExec`. The separate `datafusion-iceberg` project implements a DataFusion `TableProvider`, scan plan and append path. It is a useful design comparison, not a box on Comet's execution path.

The sibling DataFusion checkout explains `ExecutionPlan::execute(partition, TaskContext)` and `SendableRecordBatchStream`. Comet selects/adapts operators and Spark-specific semantics through its native planner. A feature implemented in upstream DataFusion is not automatically supported by Comet's serializer, eligibility rules, Spark semantics, or tests.

## Invariants worth defending in a talk

- SQL correctness spans both JVM and native components; it does not stop at the planner.
- Table format, file format, memory format and execution engine are separate layers.
- Spark cluster parallelism and native asynchronous I/O concurrency are separate controls.
- Native acceleration changes eligible implementation work, not the table's visibility contract.
- Inspect the executed plan and fallback reasons before labelling an operation native.
