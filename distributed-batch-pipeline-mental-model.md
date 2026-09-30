# Iceberg, Spark, Comet, DataFusion, and iceberg-rust

Speaker mental model for the DataFusion Comet + Iceberg talk.

Last source check: 2026-09-29. This is an architecture notebook, not a compatibility
promise. Native coverage depends on the Comet version, configuration, schema, file layout,
table properties, and the actual physical plan.

## 1. The 30-second model

Iceberg is the table format and transaction protocol. It decides which snapshot is visible,
which data and delete files belong to that snapshot, and how a new snapshot is published.

Parquet is the columnar file format. It stores column chunks inside row groups, encoded as
pages, with a footer containing schema and metadata.

Spark is the distributed SQL engine and scheduler. Its driver plans and adapts the work;
its scheduler creates stages and task attempts; executors run those tasks and retry them.

Comet is an accelerator inside Spark. It retains the Spark control plane and replaces
eligible executor-side physical regions with a native implementation.

DataFusion is Comet's executor-local Rust query engine. It runs physical operators over
Arrow RecordBatches. It is not a second distributed scheduler in this architecture.

iceberg-rust is the Rust Iceberg implementation used by Comet's native Iceberg scan and
eligible native writer path. It does not replace the Iceberg Java transaction boundary.

    Iceberg decides what data belongs to a snapshot.
    Spark decides where and when tasks run.
    Comet + DataFusion + iceberg-rust execute eligible task work natively.

## 2. Who owns what?

| Concern | Main owner | What this means |
| --- | --- | --- |
| SQL analysis, optimization, physical plan | Spark SQL / Catalyst | Comet receives a physical plan; it is not the SQL optimizer of record. |
| AQE, stages, retries, locality, speculation | Spark driver and scheduler | A native task remains a Spark task attempt. |
| Catalog, snapshot, manifests, file/delete planning | Iceberg Java through Spark | Comet receives planned FileScanTasks; it does not redo catalog planning. |
| Eligible data-file and delete-file reading | Comet native scan + iceberg-rust | Native FileIO and ArrowReader execute the supplied tasks. |
| Eligible filter, aggregate, sort, shuffle | Comet's DataFusion-based native runtime | Read the physical plan to prove the region is native. |
| In-memory columnar batches | Arrow | Arrow is a memory format, not a table or file format. |
| Eligible Parquet data-file production | Comet native writer + iceberg-rust / Parquet | Executor data-plane work can happen in parallel. |
| Validation and atomic snapshot publication | Iceberg Java on Spark driver | BatchWrite.commit remains the transaction authority. |

A useful correction when a question mixes layers:

    "That is an Iceberg metadata decision; this is a Comet execution decision."

## 3. The two planes

### Control plane: global decisions and correctness

    SQL
      -> Catalyst physical plan
      -> Iceberg Java resolves snapshot and plans FileScanTasks
      -> Spark creates stages and task attempts
      -> Spark driver coordinates completion
      -> Iceberg Java validates and commits a new snapshot, when writing

The control plane holds global state: catalog access, snapshot selection, manifest pruning,
stage boundaries, retries, task-commit coordination, and final transaction visibility.

### Executor data plane: bytes, batches, and local operators

    Task payload
      -> native scan or native child plan
      -> file reads
      -> Parquet decode
      -> Arrow RecordBatches
      -> filter / aggregate / sort / exchange
      -> Arrow ColumnarBatch or Parquet output

Most bytes are read, decoded, filtered, shuffled, aggregated, sorted, or written here.
That is the region Comet can accelerate. Both planes are in the same Spark job.

## 4. End-to-end read: SQL to distributed native execution

Example query:

    SELECT l_shipmode, SUM(l_extendedprice)
    FROM local.tpch.lineitem
    WHERE l_shipdate >= DATE '1995-01-01'
    GROUP BY l_shipmode

### 4.1 Planning flow

    1. The user submits SQL to Spark.
    2. Catalyst analyzes the table and builds a physical plan.
    3. Iceberg Java resolves the selected table snapshot.
    4. Iceberg Java reads manifest lists and manifests.
    5. Iceberg Java prunes metadata and builds FileScanTasks.
    6. Spark plans stages; AQE and runtime filters can refine execution.
    7. Comet checks scan and operator eligibility.
    8. Spark launches executor task attempts for surviving partitions.

A FileScanTask is a unit of scan work: a data-file split and the delete files that apply to
it. It is much more specific than "read this table."

For the Comet native path, Iceberg Java has already read the metadata and planned tasks.
Comet serializes task information for executors. The native FileIO reads data and delete
files; it is not asked to repeat catalog and manifest planning.

### 4.2 Executor flow

    Spark task attempt
      -> CometIcebergNativeScanExec serializes resolved task data
      -> JNI enters the Comet native runtime
      -> Comet IcebergScanExec receives pre-planned FileScanTasks
      -> iceberg-rust FileIO opens data and delete files
      -> iceberg-rust ArrowReader decodes selected Parquet columns into Arrow batches
      -> positional and equality deletes are applied
      -> eligible native filter / aggregate / sort / shuffle runs
      -> Arrow-backed ColumnarBatch returns to Spark if the plan needs it
      -> Spark completes or retries the task
      -> Spark returns the query result

Dynamic Partition Pruning remains a Spark correctness feature. Comet resolves the Spark
runtime filters before native partitions are serialized; the final native task set is the
runtime-filtered set.

## 5. What Iceberg prunes, what Parquet prunes, and what still executes

    Query predicate
      -> Iceberg partition and manifest filtering
      -> Iceberg data-file statistics filtering
      -> FileScanTasks, including applicable delete files
      -> Parquet row-group filtering
      -> projected column chunks and pages decode
      -> delete filtering
      -> residual row predicate, if needed
      -> aggregation / join / output

### 5.1 Iceberg metadata pruning

Iceberg avoids opening data files when table metadata proves they cannot match.

- The current snapshot points to manifest lists and manifests.
- A manifest contains data-file and delete-file entries.
- Data-file metadata includes path, format, record count, file size, partition tuple,
  column sizes, value/null/NaN counts, lower/upper bounds, split offsets, equality IDs,
  sort-order ID, and partition-spec ID.
- Predicates can eliminate manifests and data files before a data file is opened.
- The result is a set of FileScanTasks, including relevant delete files.

Correct language: metadata can prove "cannot match." It cannot universally prove every
remaining record matches.

### 5.2 Parquet pruning and vectorized reading

A surviving Parquet file provides another opportunity to skip work.

- A Parquet file contains row groups.
- Each row group contains a column chunk for each stored column.
- Column chunks contain encoded pages.
- The footer carries schema and row-group / column-chunk metadata.
- Projection means only referenced columns are decoded.
- Iceberg's Parquet ReadConf performs row-group tests using metrics, dictionaries, and
  Bloom filters.
- Vectorized readers materialize batches rather than one JVM object per row.

Do not say "page-level predicate pruning" in the main talk without a source and benchmark
for this exact engine. The checked Iceberg Java code explicitly performs row-group
filtering from metrics, dictionaries, and Bloom data.

### 5.3 Residual predicates

A residual is predicate work metadata could not safely eliminate. It must still be
evaluated after reading so the answer remains correct.

    Pruning removes data that cannot help.
    Residual filtering protects correctness for data that remains.

### 5.4 Delete files

Deletes are table artifacts, not rows silently removed from the Parquet data file. A
correct reader must load and apply the position/equality-delete information associated
with each task. Comet's native scan uses iceberg-rust ArrowReader specifically to apply
positional and equality deletes for merge-on-read tables.

## 6. Arrow: the in-memory contract

    Arrow RecordBatch = schema + equal-length column arrays
    Arrow array       = typed values plus validity / offset / child buffers as needed

Why it matters:

- Native operators work on vectors, not one language-level object per row.
- Compact buffers improve cache locality and make SIMD-friendly kernels practical.
- Projection avoids decoding unreferenced columns into batch memory.
- A batch can cross several native operators without reconstructing rows.

Careful claim: Arrow reduces conversion and object-allocation pressure in an eligible native
region. Do not claim every boundary is zero-copy. Type adaptation, a fallback, or a
Spark/Comet transition may materialize or convert data.

## 7. DataFusion inside Comet

DataFusion supplies Rust physical-execution concepts: ExecutionPlan, TaskContext,
RecordBatch streams, expressions, and operators. In Comet, it lives inside an executor
task.

    Spark stage
      -> Spark task attempt on executor A
      -> Comet-native execution context
      -> DataFusion operators over Arrow RecordBatches
      -> task result or shuffle data

| Question | Accurate answer |
| --- | --- |
| Does Comet turn a Spark cluster into a DataFusion cluster? | No. Spark schedules and coordinates the distributed job. |
| Does DataFusion choose the Iceberg snapshot? | No. Spark/Iceberg Java plans it for Comet's native Iceberg path. |
| Is every Spark operator native? | No. Only eligible regions convert; unsupported nodes remain Spark. |
| Where is the proof? | Spark's Comet physical plan, fallback reasons, and runtime metrics. |

The standalone datafusion-iceberg project is a useful comparison, not a runtime box in the
Comet architecture. Its IcebergTableScan builds a scan from DataFusion projection and
predicates, then streams Arrow batches. Comet instead starts from Spark/Iceberg Java's
pre-planned FileScanTasks and uses its own native scan operator. Do not draw
datafusion-iceberg as a direct dependency of Comet's Iceberg scan.

## 8. iceberg-rust: role and boundary

iceberg-rust implements Iceberg scan, Arrow read, delete handling, FileIO, catalogs, and
integrations such as DataFusion.

Comet's path is:

    Spark + Iceberg Java planning
      -> pre-planned FileScanTasks
      -> Comet native IcebergScanExec
      -> iceberg-rust FileIO + ArrowReader
      -> Arrow RecordBatches

Why this separation is useful:

- Spark's Iceberg catalog integration and planning stay authoritative.
- Spark runtime filters and DPP remain normal Spark features.
- Native code focuses on expensive file-read and batch-processing work.
- A Spark write has one transaction authority: Iceberg Java.

It does not mean iceberg-rust replans the table, replaces Spark scheduling, or makes every
Iceberg feature native.

## 9. Native write: parallel data files, JVM transaction

The write path is intentionally asymmetric:

    INSERT / overwrite / eligible copy-on-write operation
      -> Spark plan and Comet eligibility gate
      -> native child plan produces Arrow batches
      -> CometIcebergWrite produces eligible Parquet data files
      -> executor returns locations and file-result metadata
      -> driver collects successful task results
      -> Iceberg Java rebuilds TaskCommit information and validates
      -> BatchWrite.commit creates a new snapshot
      -> readers see the old or new snapshot atomically

Files can be produced in parallel, but they are not table-visible before the commit.

    Rust can speed up producing data files.
    Iceberg Java decides whether those files form the next valid snapshot.

### 9.1 Why files and commits are separate

Data-file production scales out across executor tasks. Snapshot publication is a
coordination step: validate state and install one new metadata pointer. It is much smaller
in data volume but remains driver-coordinated.

### 9.2 Copy-on-write and merge-on-read

Copy-on-write paths can be eligible when the physical plan, table version, output format,
table properties, and child plan meet Comet's gate.

Merge-on-read delta writes are not a blanket native claim. Paths that create
delta/delete artifacts outside native write interception remain JVM work. Do not say that
all DELETE, UPDATE, and MERGE variants execute in Rust.

### 9.3 Failure semantics

- Spark controls task-attempt retry.
- A failed or rejected commit cannot publish the snapshot.
- Abort/cleanup can remove known uncommitted task output.
- Orphan files can remain after some failures and are handled by maintenance later.

Atomic table visibility is different from immediate deletion of every failed temporary
object.

## 10. Maintenance: data rewrite versus metadata housekeeping

### 10.1 rewrite_data_files / compaction

Compaction is a real Comet data-plane story because it reads and rewrites data.

    RewriteDataFiles
      -> staged FileScanTasks
      -> native Iceberg scan reads data and deletes
      -> eligible native compute / shuffle / sort
      -> eligible native writer produces replacement Parquet files
      -> Iceberg Java atomically replaces the file set in a new snapshot

Current Comet tests name a bin-pack rewrite that reads file groups through
CometIcebergNativeScan. Existing coverage also includes bin-pack, sort, Z-order, and
compaction layouts with position/equality deletes, with eligible output confirmed as
native-writer output.

Why compaction can improve:

- It scans real Parquet bytes and delete artifacts.
- It can repartition, sort, and shuffle large batch volumes.
- Native scan, Arrow batches, native compute/shuffle/sort, and native file production
  lower data-plane overhead in the eligible region.

What remains unchanged: choosing rewrite groups and committing the replacement file set
are Iceberg/Spark control-plane work; final snapshot commit still has normal Iceberg
latency and conflict semantics.

For a transparent compaction benchmark, report:

- input size, file count, and average input-file size
- output size, file count, average output-file size, and target-file size
- partitioning and bin-pack/sort/Z-order strategy
- delete-file type and amount
- executor count, cores, memory, spill disk, versions, warmup/repetitions
- wall time, input bytes, files read/written, shuffle/spill metrics
- native-plan proof and correctness result

### 10.2 Metadata-only maintenance

Important but not a Comet data-plane acceleration claim by itself:

- expire_snapshots
- remove_orphan_files
- rewrite_manifests
- rewrite_position_delete_files

Rust may help in a future implementation. Today, describe Comet narrowly: it can accelerate
eligible data rewrites, not every maintenance operation.

## 11. Memory mental model

Think in executor resource envelopes, not just JVM heap.

    Executor container
      -> JVM heap
      -> Spark / Comet shared off-heap memory pool
      -> memory overhead for JVM overhead and remaining native allocations
      -> local disk for spill-capable paths

Comet's native code has a JNI CometTaskMemoryManager bridge that acquires/releases task
memory through the JVM. Comet documentation explicitly separates a configured shared
off-heap pool from executor-memory overhead needed for native memory not tracked by that
pool.

DataFusion's generic accounting model is:

    operator partition
      -> MemoryConsumer
      -> one or more MemoryReservations
      -> MemoryPool

An operator reserves before it grows state. A spill-capable operator can spill, release,
and retry after a bounded reservation fails. An operator with no spill strategy must fail
rather than allocate indefinitely. Reservations use RAII: dropping them releases tracked
memory.

| Question | Speaker answer |
| --- | --- |
| Is native memory free because it is not Java heap? | No. It still consumes the executor container. |
| Does every allocation use one perfectly observable pool? | Do not promise this. Comet documents both the shared pool and native-memory overhead. |
| Does every operator spill? | No. Spilling is operator-specific; verify the operator and metrics. |
| Why can performance regress? | Memory pressure spills, small batches/files, and conversion/fallback boundaries can dominate. |

For a demo, collect executor RSS/container metrics together with Spark/Comet spill metrics,
task time, scan bytes, and shuffle metrics. Heap-only reporting is incomplete.

## 12. Where Comet helps, and where it cannot

### Strong candidates

- Selective Iceberg Parquet scans with substantial decode/filter work.
- Aggregations after an eligible scan.
- Native shuffle/sort regions with substantial batch volume.
- rewrite_data_files jobs with eligible scan, compute, and write.
- Plans that avoid repeated row/columnar transitions.

### Reasons a gain can be small or negative

- The query is metadata-bound, object-store-latency-bound, or driver-commit-bound.
- Iceberg pruning already leaves little to process.
- Small files/tasks make per-task and JNI overhead large.
- Unsupported type, expression, scan feature, or file layout triggers fallback.
- Spark/Comet transitions dominate the remaining work.
- Native memory is too small and spills.
- An unaccelerated join, UDF, Python boundary, catalog request, or final commit dominates.

Benchmark discipline:

    Same data layout
      -> same Spark/Iceberg settings except the native switch
      -> warmup plus repeated measured runs
      -> result correctness
      -> physical-plan capture
      -> per-query distribution, not only a geometric mean

## 13. How to read a native aggregation plan

A useful native Iceberg aggregation shape is:

    CometIcebergNativeScan
      -> CometHashAggregate
      -> CometExchange / CometNativeShuffle
      -> CometHashAggregate

- CometIcebergNativeScan reads planned Iceberg file tasks natively.
- The first CometHashAggregate is local/partial aggregation.
- The exchange partitions by group key so equal keys meet.
- The final CometHashAggregate completes the aggregation.

This is the standard partial-then-final distributed aggregation pattern, not a guarantee
that every query has exactly this shape. Inspect the plan for conversion nodes and fallback
reasons before explaining a benchmark bar.

## 14. Speaker Q&A

### Is Iceberg an execution engine?

No. Iceberg is a table format and transaction layer. It plans files from snapshots,
manifests, partitions, statistics, and deletes. An engine such as Spark executes the work.

### Is Parquet the table format?

No. Parquet stores individual columnar files. Iceberg supplies table schema evolution,
snapshot history, partition metadata, delete semantics, and atomic commits over files.

### Does Comet replace Spark?

No. Spark remains optimizer, scheduler, retry authority, and job runtime. Comet replaces
eligible executor-side physical work.

### Does Comet replace Iceberg Java?

No. Iceberg Java still handles Spark catalog/table planning and remains commit authority.
Comet's native scan executes the pre-planned task data through iceberg-rust.

### Does DataFusion become the distributed engine?

No. DataFusion is embedded in native executor task work. Spark distributes, retries, and
coordinates the job.

### Why Arrow?

It is a common columnar in-memory representation. It lets native operators exchange typed
batches rather than building one object per record, and it fits vectorized Parquet decode.

### Is Spark never columnar?

No. Spark has columnar paths. A plan can still mix JVM row work, Spark columnar work,
conversions, and fallbacks. Comet's value is a contiguous eligible native region, not the
claim that Spark cannot execute columns.

### How is native scan correct with deletes?

FileScanTasks identify applicable delete files. Comet sends those tasks to iceberg-rust's
ArrowReader, which applies position and equality deletes before emitting batches.

### Does pruning change results?

No. It only skips files or row groups proven unable to match. Predicates not safely
resolved by metadata remain residual filters.

### Is native write a complete Rust Iceberg transaction?

No. Executors can produce eligible data files. Spark driver + Iceberg Java validate task
output and publish the snapshot.

### Does compaction improve?

rewrite_data_files can improve when native scan, compute, shuffle/sort, and writer output
are eligible. Metadata-only maintenance is a separate story.

### What if a feature is unsupported?

Comet records a fallback reason and Spark runs the compatible JVM path. The captured plan,
not only a configuration flag, proves which path executed.

## 15. Claims to avoid on stage

- "iceberg-rust replans the whole table in Comet."
- "DataFusion replaces Spark scheduling."
- "All Iceberg reads, writes, MERGE paths, and maintenance are native."
- "Every native boundary is zero-copy."
- "Parquet statistics always skip the desired data."
- "File production in Rust removes driver commit latency."
- "Comet makes object storage itself faster."

## 16. Source map

| Topic | Checked local source |
| --- | --- |
| Iceberg Java scan -> manifests -> FileScanTasks | apache/iceberg/core/src/main/java/org/apache/iceberg/DataTableScan.java |
| Data-file metadata carried in manifests | apache/iceberg/api/src/main/java/org/apache/iceberg/DataFile.java |
| Parquet row-group metrics/dictionary/Bloom filtering | apache/iceberg/parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java |
| Comet Iceberg eligibility and planned task extraction | datafusion-comet/spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala |
| Comet conversion to native Iceberg scan | datafusion-comet/spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala |
| Runtime-filter-aware task serialization | datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergNativeScanExec.scala |
| Native scan, iceberg-rust ArrowReader, delete FileIO | datafusion-comet/native/core/src/execution/operators/iceberg_scan.rs |
| Comet memory bridge | datafusion-comet/native/jni-bridge/src/comet_task_memory_manager.rs |
| Comet executor/off-heap/overhead model | datafusion-comet/docs/source/user-guide/latest/tuning/memory.md |
| DataFusion reservation and spill model | apache/datafusion/datafusion/execution/src/memory_pool/mod.rs |
| Standalone iceberg-rust DataFusion integration | apache/iceberg-rust/crates/integrations/datafusion/src/physical_plan/scan.rs |
| Standalone datafusion-iceberg integration | apache/datafusion-iceberg/crates/datafusion/src/physical_plan/scan.rs |

Before a new public claim, ask:

    Which layer owns this?
    Is it supported by this exact Comet version and physical plan?
    Can I show code, a plan, or benchmark evidence?
