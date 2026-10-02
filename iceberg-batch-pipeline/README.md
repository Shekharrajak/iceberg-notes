# Iceberg batch pipeline source notebook

These notes explain how Spark, Iceberg, Comet, DataFusion, and Arrow cooperate to run a distributed batch pipeline. They are a presenter reference and a source-navigation guide, with a separately qualified analysis of the saved TPC-H benchmark evidence. They are not a compatibility promise or an audited TPC-H result.

The central model is simple: Spark owns distributed query execution; Iceberg owns table-state semantics; Comet accelerates eligible executor work. Reading, filtering, delete application, schema adaptation, and writing still carry correctness obligations inside the native region.

Chapters 1-18 source check: 2026-09-30. The exact revisions, local modifications, code entry points, and test references are recorded in [the source ledger](09-source-ledger.md). No engine tests or benchmarks were executed for that original research. Chapter 19 adds captured October 2 local demo results, plans and Spark UI screenshots; these are not audited benchmarks.

For browser reading, open the [formatted notebook](index.html). It combines the complete text, tables, code blocks and inline diagrams, with chapter navigation and diagram zoom controls. Markdown remains the editable source; no server or network connection is needed to read the HTML.

## Reading map

| Chapter | Questions it answers |
| --- | --- |
| [System architecture](01-system-architecture.md) | Who owns each decision? How do the repositories fit together? |
| [Iceberg scan planning and execution](02-iceberg-scan.md) | How does a snapshot become distributed file work? Where do deletes and DPP enter? |
| [Parquet read and write concepts](03-parquet.md) | Files, row groups, column chunks, pages, encodings, indexes, projection, and batches |
| [Native runtime and serialization](04-native-runtime-and-serde.md) | Physical-plan conversion, protobuf, JNI, Arrow C interfaces, memory, and lifecycle |
| [Distributed execution and shuffle](05-distributed-execution.md) | Stages, task attempts, scheduling, AQE, network paths, local shuffle, and Celeborn |
| [Iceberg writes and commits](06-iceberg-write.md) | Parallel file production, JVM metadata reconciliation, snapshot publication, and maintenance |
| [Fault tolerance](07-fault-tolerance.md) | Task retry, shuffle recomputation, cancellation, commit conflict, unknown outcome, and cleanup |
| [Capabilities and investigation guide](08-capabilities-and-debugging.md) | Native versus delegated versus missing; what to measure and where to investigate |
| [DataFusion operators and the standalone connector](10-datafusion-and-connector.md) | How local batch streams execute; how datafusion-iceberg differs from Comet |
| [TPC-H dataset and schema](11-tpch-dataset-and-schema.md) | All eight tables and 61 columns, relationships, scale factors, data generation, Iceberg layout, and the 22-query workload |
| [TPC-H Spark versus Comet](12-tpch-spark-versus-comet.md) | Historic timings, paired Q6 plans, where time was saved, MOR evidence, harness behavior, and limits on attribution |
| [Arrow memory and kernels](13-arrow-memory-and-kernels.md) | Buffer ownership, nulls, strings/views, nested arrays, copies, alignment, FFI, and memory accounting |
| [Vectorization and hardware](14-vectorization-and-hardware.md) | Batches versus SIMD, actual Rust kernels, compiler targets, caches, bandwidth, TLBs, NUMA, and measurement |
| [Arrow in scans and rewrites](15-arrow-in-iceberg-scan-and-rewrite.md) | Decode, late materialization, delete correctness, I/O concurrency, writer buffering, compaction, and future read layout |
| [Metadata manifests snapshots and commits](16-iceberg-metadata-and-snapshots.md) | Reference sharing, metadata pruning, snapshot/file sequences, catalog atomicity, optimistic retries, retention and inspection SQL |
| [Distributed Iceberg and compaction](17-distributed-iceberg-and-compaction.md) | Remote manifest planning, driver limits, task/group concurrency, JVM/native boundaries, partial progress and concurrent deletes |
| [Integration tests and benchmark evidence](18-iceberg-tests-and-benchmark-evidence.md) | Exact test assertions, historical XML findings, benchmark timing boundaries, focused commands and missing scale evidence |
| [Reading Spark UI](19-reading-spark-ui.md) | Normal versus forced shuffle; Q1/Q3/Q9/Q18 plans, timings, annotated screenshots, DAG/stage/executor interpretation and limits |
| [Source ledger](09-source-ledger.md) | Exact checkouts, source links, tests, limitations, and revalidation procedure |

The existing [earlier mental-model notebook](../distributed-batch-pipeline-mental-model.md) is preserved. Prefer this collection for the qualified implementation details below.

## Diagram artifacts

The technical chapters contain editable Mermaid flowcharts, sequence diagrams and an entity-relationship diagram. [The offline diagram gallery](diagrams/index.html) shows the rendered SVGs and links to the corresponding `.mmd` sources. The renderer extracts its inputs from the Markdown; edit the notes first, then regenerate. See [rendering instructions](scripts/README.md).

Blue denotes Spark control/runtime, yellow Iceberg metadata, purple Comet/DataFusion native execution, orange file/codec work, green executor work, and slate durable storage or external services. Diagrams are conceptual traces of the cited paths, not exhaustive call graphs. Async scheduling, I/O overlap, and version shims are compressed where they do not change the ownership being explained.

## Findings to keep straight

| Finding | Consequence |
| --- | --- |
| Comet uses its own Iceberg scan and directly calls Iceberg Rust | Do not put the separate datafusion-iceberg connector in Comet's dependency chain |
| Iceberg Java plans file tasks before native execution | Native readers do not independently choose the snapshot or replan manifests |
| Driver-side task serialization observes resolved runtime filters | Do not describe DPP serialization as executor-side table planning |
| The native Iceberg reader enables page-index row selection | Do not carry the narrower Java ReadConf row-group-only explanation over to the Rust path |
| Executor JVM code rebuilds file metadata and TaskCommit objects | The driver collects those messages and performs the table commit |
| Unknown commit outcome differs from a known rejected commit | Do not delete files simply because the client received an exception |
| Delete-file rewriting reads/writes data artifacts | `rewrite_position_delete_files` is not purely metadata housekeeping; it is not thereby natively accelerated |
| Native reads and native writes have separate eligibility gates | v3 delete-vector reads do not imply v3 native writes |
| Fallback is normally a planning decision | Runtime native errors generally fail a task; arbitrary mid-query JVM fallback is not promised |
| Arrow layout, batch execution and SIMD are distinct | Check concrete kernels, types, compiler targets and profiles before attributing performance |
| The Arrow reference tree is 60.0.0; Comet resolves 59.3.0 | Newer reference-source optimizations are not automatically in the current Comet binary or old benchmark |
| A faster rewrite and faster post-rewrite reads are separate outcomes | Measure rewrite CPU/I/O/commit cost separately from file layout and future pruning |
| Distributed manifest planning still returns descriptors to the driver | Remote metadata processing does not imply constant driver memory |
| Compaction can retain the starting data sequence number | Newer equality deletes remain applicable; position-delete conflicts still require validation |
| A benchmark named rewrite may measure metadata only | Inspect the timed block before comparing it with physical row compaction |

## Evidence discipline

- **Source-backed** means traced through the recorded implementation; a named test demonstrates coverage intent, not a test run in this session.
- **Illustrative** means a teaching example, such as hypothetical file counts or a simple two-stage aggregate.
- **Inference** means an engineering opportunity or performance explanation derived from the source, not a measured gain or roadmap commitment.
- The local Spark branch contains additional pipelined-shuffle work. The main distributed diagrams deliberately describe the conventional materialized shuffle path, not those branch-specific experimental paths.
- Iceberg Spark integration examples use the v3.5 module unless noted. Comet has version shims. The separate Spark/DataFusion/Arrow working trees are reference sources, not proof that those revisions are linked into this Comet build.

## The one sentence to remember

```text
Iceberg selects valid table work -> Spark distributes and retries it -> Comet executes eligible native regions -> Iceberg Java publishes valid write results
```
