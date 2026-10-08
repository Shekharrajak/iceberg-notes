# Source revisions evidence and validation limits

[Index](README.md)

## Checkout scope

Checked on 2026-09-30. These are local source revisions, not a claim that every repository is at current upstream HEAD or linked into the same binary.

| Repository | Local root | HEAD |
| --- | --- | --- |
| Comet | `/Users/srajak/Documents/repos/oss/apache/datafusion-comet` | `184accac5b9cee6b761a6673c73c263adedef45e` |
| Spark | `/Users/srajak/Documents/repos/oss/apache/spark` | `f9358a5587a2d512c5cf08ba4b10d60007c93f6e` |
| Iceberg Java | `/Users/srajak/Documents/repos/oss/apache/iceberg` | `5e7169168db3d34e29354c6f59ec4d6e420b8d2d` |
| DataFusion | `/Users/srajak/Documents/repos/oss/apache/datafusion` | `cd05b417544262f8a6c114e304055da53e5b4162` |
| datafusion-iceberg | `/Users/srajak/Documents/repos/oss/apache/datafusion-iceberg` | `b12e9871dd2b2632cd05786d9f12dd6cde47dc2e` |
| arrow-rs | `/Users/srajak/Documents/repos/oss/apache/arrow-rs` | `b89e3020d9ad6c5fc5c9e9acafc83fb21866f536` |
| Iceberg Rust used by Comet | Cargo git checkout `iceberg-rust-1cfaaa0dd97c960f/bb1e4a4` | `bb1e4a4861f02377489eff818b75138f414c4cb0` |

[Comet dependency declarations](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/Cargo.toml:40) specify DataFusion 55.1.0, Arrow/Parquet 59.2.0 and the Iceberg Rust revision above. The separate DataFusion and Arrow checkouts are conceptual/API references, not substitutes for verifying resolved build dependencies.

The Comet worktree has existing local edits in the Iceberg scan serde and native scan test suite, plus two benchmark files. The notable semantic change allows safe partial residual weakening using predicate polarity. It is explicitly labelled uncommitted throughout the notes. No source edits were made for this notebook. Spark has unrelated untracked Kafka work; Iceberg has unrelated untracked research files. Those were left untouched.

The initial notebook creation preserved the older note and presentation. A subsequent source-backed correctness audit updates their manifest, driver/executor, commit-outcome and benchmark explanations alongside the relevant chapters. Publishing these documentation corrections does not imply a new benchmark, engine build or engine test verdict.

## Iceberg planning

| Entry point | Evidence |
| --- | --- |
| [DataTableScan.doPlanFiles](/Users/srajak/Documents/repos/oss/apache/iceberg/core/src/main/java/org/apache/iceberg/DataTableScan.java:64) | Selected snapshot data/delete manifests and ManifestGroup construction |
| [ManifestGroup.planFiles](/Users/srajak/Documents/repos/oss/apache/iceberg/core/src/main/java/org/apache/iceberg/ManifestGroup.java:177) | Residual caches, delete index and file scan task creation |
| [DeleteFileIndex.forEntry and forDataFile](/Users/srajak/Documents/repos/oss/apache/iceberg/core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:150) | Sequence/partition/path-based delete association |
| [TableScanUtil](/Users/srajak/Documents/repos/oss/apache/iceberg/core/src/main/java/org/apache/iceberg/util/TableScanUtil.java:57) | Split/group work and open/delete-cost weights |
| [SparkScanBuilder](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java:153) | Filter/projection pushdown and aggregate metadata path |
| [SparkPartitioningAwareScan](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkPartitioningAwareScan.java:178) | Materialize tasks and group for Spark |
| [SparkBatchQueryScan](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:105) | Runtime partition filtering |
| [SparkBatch](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatch.java:96) | Input partitions and reader-factory eligibility |
| [Spark BatchScanExec](/Users/srajak/Documents/repos/oss/apache/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/BatchScanExec.scala:39) | Scan-to-batch, filtered partitions and DataSourceRDD |

Iceberg integration examples deliberately identify the v3.5 source module. Consult the matching module and Comet shim before asserting behavior for another Spark version.

## Comet scan

| Entry point | Evidence |
| --- | --- |
| [CometScanRule](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala:443) | Recognized Iceberg scans, schema/version/metadata/delete/DPP gates |
| [Iceberg scheme allowlist](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala:1214) | Readable storage schemes, distinct from generic Parquet support |
| [Scan metadata field allowlist](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeScan.scala:100) | Four exposed metadata fields |
| [Residual conversion](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeScan.scala:652) | Predicate/type limits and local polarity change |
| [Partition serialization](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeScan.scala:945) | Shared pools, task slices, historical/delete-required fields |
| [CometIcebergNativeScanExec](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergNativeScanExec.scala:87) | DPP-resolved serialization, unknown partitioning, no ordering |
| [Native Iceberg plan construction](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/planner.rs:1852) | Custom IcebergScanExec, not generic Parquet datasource |
| [Native Iceberg scan execution](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_scan.rs:175) | FileIO, delete-size stats, concurrency, page selection, schema adaptation |
| [Native Iceberg storage](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_common.rs:51) | Storage factories, read/write distinction and credential integration |
| [Comet configuration](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometConf.scala:114) | Scan/write defaults and file concurrency |

## Iceberg Rust reader

These links point to the exact Cargo checkout pinned by Comet, not the separate local iceberg-rust branch.

| Entry point | Evidence |
| --- | --- |
| [ArrowReaderBuilder](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/mod.rs:65) | Concurrency, row-group/page/bloom options and defaults |
| [Reader pipeline](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/pipeline.rs:66) | Task stream execution and concurrency |
| [File processing](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/pipeline.rs:139) | Footer, IDs, name mapping, projection and lineage metadata |
| [Empty projection handling](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/pipeline.rs:372) | Difference between metadata-only projection and empty COUNT-style projection |
| [Delete and selection composition](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/pipeline.rs:557) | Combined equality predicate, row groups, row selections and position deletes |
| [Byte-range row-group ownership](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/row_filter.rs:183) | Half-open row-group midpoint ownership across splits |
| [ArrowFileReader](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/file_reader.rs:58) | Byte reads, range coalescing and bounded range-fetch concurrency |

Portable upstream root for these files: [Iceberg Rust at the pinned revision](https://github.com/apache/iceberg-rust/tree/bb1e4a4861f02377489eff818b75138f414c4cb0).

## Parquet

| Entry point | Evidence |
| --- | --- |
| [Java ReadConf](/Users/srajak/Documents/repos/oss/apache/iceberg/parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java:64) | Schema ID strategies and explicit row-group metrics/dictionary/bloom pruning |
| [Parquet crate concepts](/Users/srajak/Documents/repos/oss/apache/arrow-rs/parquet/src/lib.rs:28) | File layout and Arrow integration |
| [Arrow reader builder](/Users/srajak/Documents/repos/oss/apache/arrow-rs/parquet/src/arrow/arrow_reader/mod.rs:326) | Independent batch size, row groups, projection, row selection and row filter |
| [ArrowWriter](/Users/srajak/Documents/repos/oss/apache/arrow-rs/parquet/src/arrow/arrow_writer/mod.rs:360) | Batch write, row-group flush and file close |
| [Writer properties](/Users/srajak/Documents/repos/oss/apache/arrow-rs/parquet/src/file/properties.rs:247) | Independent page, row-group, encoding/compression settings |

The nested-layout explanation is a conceptual explanation of Parquet/Arrow representations. The notebook does not claim all encodings or index optimizations are enabled by Comet.

## Comet runtime

| Entry point | Evidence |
| --- | --- |
| [CometSparkSessionExtensions](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometSparkSessionExtensions.scala:39) | AQE/columnar extension ordering and Spark-version distinctions |
| [CometExecRDD](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometExecRDD.scala:56) | RDD dependencies, partition payloads and input slots |
| [CometExecIterator](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometExecIterator.scala:85) | Native setup, task context, batch iteration, error conversion and cleanup |
| [Native createPlan](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/jni_api.rs:555) | Protobuf/config decoding, task memory and native session setup |
| [Native output FFI](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/jni_api.rs:934) | Arrow export, row count and non-zero-offset copy path |
| [Native executePlan](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/jni_api.rs:1090) | Stream driving and output lifecycle |

## Memory

| Entry point | Evidence |
| --- | --- |
| [JVM CometTaskMemoryManager](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java:41) | Acquire/release execution budget and no-op native spill callback |
| [JNI memory bridge](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/jni-bridge/src/comet_task_memory_manager.rs:28) | Native calls into JVM acquire/release methods |
| [Comet memory explanation](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/docs/source/user-guide/latest/tuning/memory.md:26) | Supporting documentation for tracked reservations versus untracked footprint |
| [DataFusion memory pool](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/execution/src/memory_pool/mod.rs:18) | Consumer, reservation and pool abstraction |

## Spark scheduling

| Entry point | Evidence |
| --- | --- |
| [DAGScheduler stage submission](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala:2665) | Parent-stage dependencies and scheduling |
| [Task binary and task creation](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala:3156) | Shared broadcast task binary and per-partition tasks |
| [DAGScheduler completion and fetch failures](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala:3645) | Task completion, output invalidation and stage recovery |
| [TaskSchedulerImpl](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala:299) | TaskSet submission and resource offers |
| [TaskSetManager failure handling](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:992) | Failure classification, counted attempts, pending retries |
| [Scheduler backend launch](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala:457) | TaskDescription serialization and LaunchTask message |
| [Executor backend](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala:181) | LaunchTask reception and status reporting |
| [Executor TaskRunner](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/executor/Executor.scala:695) | Task attempt execution and result/failure lifecycle |
| [AdaptiveSparkPlanExec](/Users/srajak/Documents/repos/oss/apache/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala:197) | Query-stage optimization and replanning |

The branch-specific `PipelinedShuffleDependency` paths in this Spark checkout are excluded from the conventional recovery diagrams. Do not treat those experimental paths as the behavior of Comet's default shuffle.

## Spark networking

| Entry point | Evidence |
| --- | --- |
| [MapOutputTracker](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/MapOutputTracker.scala:678) | Block-location/size lookup and map-output registration |
| [NettyBlockTransferService](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala:119) | Block transfer and RetryingBlockTransferor |
| [ShuffleBlockFetcherIterator](/Users/srajak/Documents/repos/oss/apache/spark/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala:1) | Local/remote fetch handling, in-flight limits and failures |

## Shuffle

| Entry point | Evidence |
| --- | --- |
| [CometNativeShuffleInputRDD](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleInputRDD.scala:34) | Scheduling anchor, dependency preservation, task payload and determinism |
| [CometNativeShuffleWriter](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala:45) | Unified native writer plan, local output commit and remote destination |
| [CometBlockStoreShuffleReader](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala:39) | Spark fetcher limits, JVM decode path and raw-stream path |
| [CometShuffleBlockIterator](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/java/org/apache/comet/CometShuffleBlockIterator.java:34) | Framing, reusable DirectByteBuffer and lifetime contract |
| [ShuffleScanExec](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/shuffle_scan.rs:49) | Native compressed-block pull, decode and thread restrictions |
| [ShuffleBlockWriter](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/shuffle/src/writers/shuffle_block_writer.rs:65) | Self-contained compressed IPC block format |
| [IPC decoder](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/shuffle/src/ipc.rs:34) | Schema/dictionary decoding and remote validation option |
| [Celeborn materialization](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometCelebornShuffleMaterialization.scala:38) | Driver-owned destination selection and narrowly scoped local fallback |

## Writes

| Entry point | Evidence |
| --- | --- |
| [IcebergWriteStrategy](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/iceberg/IcebergWriteStrategy.scala:37) | Append/overwrite/ReplaceData interception and two-operator shape |
| [Native writer gates](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeWrite.scala:168) | Format, version, type, properties, FileIO and location restrictions |
| [CometIcebergWriteExec](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergWriteExec.scala:175) | Executor-side payload decode, cleanup ownership, metrics and TaskCommit |
| [Native Iceberg writer](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:452) | Writer stack, field IDs, partition routing, close and cleanup |
| [Attempt-specific file prefix](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:883) | Partition/task-attempt/operation naming |
| [IcebergCommitExec](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/IcebergCommitExec.scala:74) | Collect completed task messages, commit, abort and known-file cleanup |

## Iceberg commits

| Entry point | Evidence |
| --- | --- |
| [SparkWrite commit and abort](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:212) | Branch/WAP targeting, cleanable-failure cleanup and BatchWrite contract |
| [SparkWrite append](/Users/srajak/Documents/repos/oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:296) | DataFiles handed to Java append update |
| [SnapshotProducer.commit](/Users/srajak/Documents/repos/oss/apache/iceberg/core/src/main/java/org/apache/iceberg/SnapshotProducer.java:484) | Bounded conflict retry, TableOperations commit and unknown-outcome protection |

## DataFusion and Arrow

| Entry point | Evidence |
| --- | --- |
| [ExecutionPlan](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/physical-plan/src/execution_plan.rs:67) | Executor-local partition stream contract |
| [RecordBatchStream](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/execution/src/stream.rs:26) | Async stream of RecordBatch results |
| [FilterExec execution](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/physical-plan/src/filter.rs:547) | Wrap child partition stream and evaluate predicate |
| [Aggregate modes](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/physical-plan/src/aggregates/mod.rs:142) | Raw input, intermediate state, partial/final aggregate modes |
| [Hash join execution](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/physical-plan/src/joins/hash_join/exec.rs:257) | Build/probe execution and partitioning contracts |
| [External sort](/Users/srajak/Documents/repos/oss/apache/datafusion/datafusion/physical-plan/src/sorts/sort.rs:95) | Sorted runs, reservation pressure, spill and merge |
| [RecordBatch](/Users/srajak/Documents/repos/oss/apache/arrow-rs/arrow-array/src/record_batch.rs:224) | Schema, arrays and row-count representation |
| [Arrow FFI](/Users/srajak/Documents/repos/oss/apache/arrow-rs/arrow-array/src/ffi.rs:237) | Export/import C Data interface |
| [Separate connector TableProvider](/Users/srajak/Documents/repos/oss/apache/datafusion-iceberg/crates/datafusion/src/table/mod.rs:128) | Fresh table load, inexact pushdown, append-only insert support |
| [Separate connector scan](/Users/srajak/Documents/repos/oss/apache/datafusion-iceberg/crates/datafusion/src/physical_plan/scan.rs:109) | Single unknown output partition and direct table scan stream |
| [Separate connector commit](/Users/srajak/Documents/repos/oss/apache/datafusion-iceberg/crates/datafusion/src/physical_plan/commit.rs:188) | Collect coalesced file descriptors and commit through Rust transaction/catalog |

## Tests inspected

| Test source | Useful cases |
| --- | --- |
| [CometIcebergNativeSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergNativeSuite.scala:561) | Deletes, shared DVs, page skipping, schemas, DPP, metadata, split ownership and metrics |
| [CometIcebergResidualPushdownSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergResidualPushdownSuite.scala:1) | Predicate conversion correctness |
| [CometIcebergWriteActionSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergWriteActionSuite.scala:493) | Abort, concurrent commit conflict, native engagement and metadata parity |
| [MERGE contrasting plan cases](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergWriteActionSuite.scala:929) | JVM MergeRows can coexist with plan-dependent native file writing |
| [CometIcebergWriteDetectionSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergWriteDetectionSuite.scala:1) | Native write eligibility gates |
| [CometIcebergRewriteActionSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:1) | Maintenance scan/write integration |
| [CometExecIteratorLifecycleSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/spark/CometExecIteratorLifecycleSuite.scala:81) | Setup failure, idempotent cleanup and failed teardown |
| [PlanDataInjectorShuffleLifecycleSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/spark/sql/comet/PlanDataInjectorShuffleLifecycleSuite.scala:103) | Plan-data reuse and shuffle lifecycle cleanup |
| [CometNativeShuffleWriterSuite](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriterSuite.scala:36) | Native writer parameter propagation |

These are source/test inspections, not newly executed test results. Render and link validation for this documentation is recorded separately in [VALIDATION.md](VALIDATION.md).

## Arrow and hardware addendum

Chapters [13](13-arrow-memory-and-kernels.md), [14](14-vectorization-and-hardware.md) and [15](15-arrow-in-iceberg-scan-and-rewrite.md) extend the source inspection into Arrow buffers/kernels, hardware cost models, and scan/rewrite integration. Their primary-source links and inspected tests are recorded next to the claims.

| Source | Revision and interpretation |
| --- | --- |
| Arrow reference checkout | `149b3ab23dc360e9452b2f854b01d2f5fed06f43`, clean; workspace 60.0.0 |
| Comet dependency boundary | `184accac5b9cee6b761a6673c73c263adedef45e`; Arrow/Parquet manifest requirements 59.2.0, resolved lockfile versions 59.3.0; existing user changes preserved |
| Iceberg Rust used by Comet | Manifest-pinned `bb1e4a4861f02377489eff818b75138f414c4cb0`; inspected the corresponding Cargo git checkout, not the separate datafusion-iceberg connector |
| DataFusion reference checkout | `cd05b417544262f8a6c114e304055da53e5b4162`, clean; not a claim about historical binary linkage |
| Iceberg Java reference checkout | `5e7169168db3d34e29354c6f59ec4d6e420b8d2d`; pre-existing untracked research notes preserved |

The cached 59.3.0 aggregation/filter implementations were also checked. The BMI2 bitmap `compress`/`expand` helpers in Arrow 60.0.0 were absent from that cached 59.3.0 buffer source; they must not be credited to the checked Comet dependency. Local Cargo source links require those caches to remain available. Release numbers alone do not prove which binary ran the saved benchmark.

Hardware explanations use official LLVM, Rust, Intel, Arm, Linux and Berkeley Lab references. They are explanatory cost models, not measurements of this machine. No assembly, hardware counters, Arrow tests or rewrite benchmarks were collected/run for this addendum.

## Metadata and distributed compaction addendum

Chapters [16](16-iceberg-metadata-and-snapshots.md), [17](17-distributed-iceberg-and-compaction.md) and [18](18-iceberg-tests-and-benchmark-evidence.md) trace the same recorded Iceberg and Comet HEADs through manifest reuse, catalog commit mechanisms, distributed manifest planning, rewrite commit groups and concurrent-delete validation. Relevant implementation and test links appear next to each claim.

The inspection found v4 manifest code in the local Iceberg reference tree. V2/v3 examples and Avro-manifest descriptions must not be interpreted as an exhaustive description of v4, or as a claim that Comet supports those newer native-write paths. The local Spark 3.5 runtime integration class is `TestRoundTrip`; cross-version guides may name a different smoke test.

Existing local XML reports were inspected separately from test source. They have mixed dates and unverified binary/source provenance, including one selected scan test cancelled when SparkContext shut down. No current engine test pass, benchmark gain or live CI status is asserted. Benchmark analysis distinguishes metadata-only replacement/commit work from real Spark row compaction and flags execution-enabled comparisons that do not isolate scan CPU alone.

## Revalidation procedure

1. Record every relevant checkout HEAD and dirty status again.
2. Read Comet's resolved dependency revisions; do not assume sibling main branches match.
3. Recheck scan/write/storage gates before updating capability cells.
4. Read the matching Spark shim and Iceberg Spark module for the intended runtime version.
5. Confirm native plan engagement and correct results with focused tests or a controlled query.
6. Confirm I/O behavior before claiming pruning, and exact failure outcomes before claiming recovery.
7. Regenerate diagrams after editing Mermaid and inspect the changed outputs.

Historical TPC-H timings are separately qualified in chapter 12. No new SIMD, hardware or rewrite speedup, or live GitHub issue/PR state, is asserted by this addendum.


## October 8 compaction and table-health addendum

Chapters 21-22 pin algorithm references to Iceberg `apache-iceberg-1.8.1` (`aef7c249e077ee8cd64489fe0aa6fd52227647bd`) and the Spark V2 write protocol to `v3.5.3`, matching runtime versions. The newer local Iceberg checkout is `c24eeea0b11a373c81a8cc9c8517df9a6ea84a6e`; its planner/runner class names differ and are not substituted for the benchmark's call graph. The local Spark reference HEAD is `f9358a5587a2d512c5cf08ba4b10d60007c93f6e`, separate from the 3.5.3 runtime.

Comet inspection uses local HEAD `184accac5b9cee6b761a6673c73c263adedef45e` with existing scan/benchmark edits. Its Cargo-pinned iceberg-rust source is `bb1e4a4861f02377489eff818b75138f414c4cb0`, inspected in the Cargo checkout. The separate iceberg-rust repository at `60513fdcd152d71c388b1c60b8d02c91dad85693` is not treated as the linked dependency. Binary hashes, not these checkout labels, identify the measured JARs.

The new [curated benchmark evidence](assets/compaction/compaction-16m.json) preserves all 30 measured timings, 30 warm-ups with correctness records, input file-manifest hashes, six median-trial physical plans, and JAR SHA-256 hashes. See [chapter 21](21-compaction-algorithms-and-execution.md) for source/test links and [chapter 22](22-table-health-and-compaction-benchmark.md) for method and limits. No new engine tests or benchmark jobs were run for this documentation update.


## October 8 CometSort addendum

[Chapter 23](23-comet-sort-sql-to-rust.md) follows the recorded Comet checkout through Scala serialization, task JNI, the native planner and the resolved `datafusion-physical-plan 55.1.0` / `arrow-ord 59.3.0` dependency sources. DataFusion `sort.rs`, `stream.rs`, `streaming_merge.rs` and `merge.rs`, plus Arrow `sort.rs`, were compared byte for byte with their local cached `.crate` archives; all matched. This checks local source consistency, not the exact source of the October 7 binary.

Source fingerprints: DataFusion `sorts/sort.rs` SHA-256 `e731d6fa1e5d68a36154ba0fd497042af19d010f922c6277c3c5492be70a7cfc`; Arrow `sort.rs` SHA-256 `b60edee04fa10dff2c9cb85f334cf88fb6d6b4c61d4f877acae4e1628299a720`. Full source and inspected-test links appear alongside the explanations. Spark baseline sorting refers to the official `v3.5.3` source.

Chapter 21's sorting reference now points to the native planner and DataFusion operator. Comet's `native/core/src/execution/sort.rs` is a shuffle partition-ID radix helper, not the SQL sorter. The new chapter distinguishes the active chunked sort path from the simpler `sort_batch` helper, task-local ordering from range distribution, and full-sort from Top-K. The saved 2.31x whole-compaction result is not attributed to sorting alone.
