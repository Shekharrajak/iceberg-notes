# Compaction algorithms and execution internals

Iceberg chooses the files and publishes the replacement table state. Spark plans and schedules the distributed work. Comet can replace the eligible scan, shuffle, sort and Parquet writer inside that work. The compaction policy remains Iceberg's.

Read this chapter for the algorithms and code. Continue with [table health and the benchmark lab](22-table-health-and-compaction-benchmark.md) for inspection SQL, measurements, maintenance decisions and a demo sequence. For the complete SQL-to-Rust sorting path, see [CometSort internals](23-comet-sort-sql-to-rust.md). These chapters add the October 7 benchmark to the earlier [distributed compaction notes](17-distributed-iceberg-and-compaction.md).

## The ownership model

| Question | Owner | Important consequence |
| --- | --- | --- |
| Which snapshot and files are candidates? | Iceberg Java scan and rewrite action | File pruning, partition specs and delete applicability precede native execution |
| Which files belong in a rewrite group? | Iceberg size-based rewriter | Comet does not invent a different bin-packing policy |
| How many tasks, exchanges and attempts execute? | Spark with Iceberg's distribution requirements | One rewrite group can use many Spark tasks |
| Who reads/deletes/sorts rows? | JVM operators or eligible Comet operators | The physical plan establishes which path actually ran |
| Who encodes replacement Parquet? | Iceberg Java/parquet-mr, or Comet/iceberg-rust/parquet-rs | Native write means actual file production in Rust |
| Who publishes the new snapshot? | Iceberg Java through the catalog/TableOperations contract | Native file creation does not replace transaction semantics |

```text
Action: SQL CALL -> Iceberg file selection -> rewrite groups -> Spark jobs -> Iceberg replacement commit
JVM data: staged scan -> JVM rows/operators -> Iceberg Java writer -> task commit messages
Comet data: native scan -> optional native exchange/sort -> native Parquet write -> JVM task commit messages
```

**Source scope, checked 2026-10-08:** Iceberg algorithm references below are pinned to the `apache-iceberg-1.8.1` tag (`aef7c249e077ee8cd64489fe0aa6fd52227647bd`), matching the runtime JAR version. Spark write protocol references use `v3.5.3`. Comet references use the local checkout at `184accac5b9cee6b761a6673c73c263adedef45e`, with existing local scan changes. Its Cargo manifest pins iceberg-rust `bb1e4a4861f02377489eff818b75138f414c4cb0`; dependency details were checked in that Cargo checkout. The separate Iceberg HEAD `c24eeea0b11a373c81a8cc9c8517df9a6ea84a6e` has newer planner/runner names. Do not use that HEAD's class names as the 1.8.1 call graph. JAR hashes in chapter 22 identify measured binaries; the working Git revision alone does not prove their exact source.

No engine tests or benchmark jobs were rerun while writing these chapters. Test references below describe inspected assertions, not new passing test results.

## 1. What compaction really rewrites

A Parquet data file is immutable. A data compaction reads selected files, reconstructs their visible rows, and writes new files. It is not normally byte-concatenation of `.parquet` objects: each file has its own footer, row groups, encoding choices and statistics. Applying deletes or changing row order also requires interpreting the records.

There are three different sizes to keep separate:

- **Compressed file bytes:** what Iceberg uses for much of the rewrite planning and file-size policy.
- **Decoded rows / Arrow batches / sort state:** what consumes task memory. A 128 MiB Parquet file can decode to much more than 128 MiB.
- **Replacement compressed bytes:** depend on surviving rows, ordering, encoding, codec and writer behavior. They need not equal input bytes or match between engines.

A rewrite normally preserves the logical table rows at its valid commit point. A delete-aware rewrite may write fewer *physical* records because the old snapshot already hid some records through delete files. That is not a new logical DELETE.

## 2. Iceberg Java: from CALL to candidate groups

```text
RewriteDataFilesProcedure.call -> SparkActions.rewriteDataFiles -> RewriteDataFilesSparkAction.execute
Snapshot scan -> filter candidate files -> group by partition -> plan bounded file groups -> execute groups
```

`RewriteDataFilesProcedure.call` parses the table, strategy, sort order, `where` expression and option map, then invokes the action. `execute` captures the starting snapshot, initializes options and chooses bin-pack if no strategy is supplied. An empty table or no eligible group returns without rewriting data. Sources: [RewriteDataFilesProcedure.java:102](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java#L102), [RewriteDataFilesSparkAction.java:159](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java#L159).

The action calls `newScan().useSnapshot(...).filter(...).ignoreResiduals().planFiles()`. This is a crucial distinction: `where` restricts **which files can be selected**, not which rows of a selected file should survive compaction. Residual row filtering is intentionally ignored. Otherwise a maintenance call such as `where => 'customer_id < 100'` could silently discard other rows from the same selected file.

The action groups tasks by the current partition tuple. Files whose spec differs from the current table spec are grouped specially because their contents may span multiple output partitions. The output writer must still respect the chosen output spec. It does not simply merge unrelated partition values into one ordinary partitioned data file. Source: [RewriteDataFilesSparkAction.java:201](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java#L201).

### Candidate file and group selection

The following is teaching pseudocode for the 1.8.1 implementation:

```text
candidate(file) = rewrite_all
                 OR file.size < min_size
                 OR file.size > max_size
                 OR applicable_delete_file_count >= delete_file_threshold
                 OR known_file_scoped_delete_ratio >= delete_ratio_threshold

groups = pack(candidate_files_per_partition, max_file_group_size)

rewrite(group) = rewrite_all
                OR (group.count > 1 AND group.count >= min_input_files)
                OR (group.count > 1 AND group.bytes > target_size)
                OR group.bytes > max_size
                OR any file passes a delete threshold
```

Defaults in this version: minimum size `0.75 * target`, maximum candidate size `1.80 * target`, minimum input count `5`, maximum group bytes `100 GiB`, delete-file-count threshold `Integer.MAX_VALUE`, delete-ratio threshold `0.3`. The target comes from the action option or table write target. These are policy defaults, not universal recommended values. Sources: [SizeBasedFileRewriter.java:54](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewriter.java#L54), [SizeBasedDataRewriter.java:107](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/SizeBasedDataRewriter.java#L107).

The delete ratio is an estimate from **file-scoped** delete-file record counts, capped at the data file's record count. It is not `SUM(all delete records) / SUM(all data records)` and is not a distinct deleted-row count. Duplicate positions and equality deletes make simplistic global ratios misleading. Our benchmark uses `rewrite-all=true`, so it bypasses these eligibility thresholds; its 5% deletion density does not mean the default 30% threshold triggered the run.

### There are two levels of packing

| Level | Unit | Purpose |
| --- | --- | --- |
| Rewrite group | Collection of candidate file tasks within a partition/spec context | Bound work per group, scheduling concurrency and commit units |
| Spark scan task group | File/split tasks assigned to one Spark input partition | Set per-task read work and influence output file sizes |

`SizeBasedFileRewriter.planFileGroups` uses `BinPacking.ListPacker(maxGroupSize, 1, false)`. `BinPacking` is a bounded-lookback greedy fit: find an open bin that can accept the weight; otherwise create one and emit an older/largest bin according to policy. It is not an optimal bin-packing solver, and `largestBinFirst` chooses a bin to emit rather than sorting all input files into decreasing-size order. Later scan-task packing has its own lookback and weighting. Sources: [SizeBasedFileRewriter.java:167](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewriter.java#L167), [BinPacking.java:95](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/util/BinPacking.java#L95), [TableScanUtil.java:100](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/util/TableScanUtil.java#L100).

## 3. Bin-pack algorithm

**Intent:** reduce wrongly sized files with little ordering work.

```text
Selected file group -> pack scan tasks near a split target -> read visible rows -> write larger files
```

`SparkBinPackDataRewriter` sets `SCAN_TASK_SET_ID`, a computed `SPLIT_SIZE`, and `FILE_OPEN_COST=0` on the read. The write receives the rewrite group ID, an enlarged writer size target, the output spec and a distribution mode. Normal same-spec compaction requests `NONE`; a spec change can require `RANGE` distribution. Therefore, “bin-pack never shuffles” is too broad. Our unpartitioned benchmark's captured bin-pack plans contain no exchange or sort. Source: [SparkBinPackDataRewriter.java:43](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackDataRewriter.java#L43).

Data files smaller than a split remain whole units in this workload. Larger splittable files can produce scan splits; Parquet row-group ownership determines which rows each split actually reads. “A task equals exactly one file” is not a general Spark/Iceberg rule.

### Output-count and size heuristics

Let `S` be group input bytes and `T` the target file bytes. For the default thresholds:

```text
minimum candidate size = 0.75 * T
maximum candidate size = 1.80 * T
writer size target     = T + 0.5 * (maximum candidate size - T) = 1.40 * T

if S < T: estimated outputs = 1
otherwise:
  up   = ceil(S / T)
  down = floor(S / T)
  if remainder(S, T) > minimum candidate size: use up
  else if S / down < min(1.1 * T, writer size target): use down
  else: use up

split target = clamp(S / estimated_outputs + 5 KiB, T, writer size target)
```

This avoids intentionally producing a tiny tail file when spreading the remainder across other files would keep them near target. The estimate is based on input compressed bytes, not a precise prediction of future compressed size. `TARGET_FILE_SIZE_BYTES` passed to the per-group writer can therefore exceed the user-facing compaction target. Source: [SizeBasedFileRewriter.java:186](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewriter.java#L186).

### Why our run produced 10 files, not 9

Observed input: `1,267,756,086 bytes / 256 files = 4.72 MiB per file`, with `T = 128 MiB`.

```text
Input = 1209.03 MiB -> estimate 9 output files -> split target about 134.34 MiB
Whole small input files -> 28 files per full bin -> 9 full bins + 4-file tail -> 10 tasks/files
```

The source formula explains why **9 is the desired estimate**. Discrete file packing explains why the actual bin-pack plan uses **10** tasks. Recorded output counts and row counts confirm 9 large files plus one small tail in the clean bin-pack case. With 62,500 rows per seed file, a 28-file group contains 1,750,000 rows and the four-file tail contains 250,000. This is a run-specific explanation, not a promised output count.

Comet changes execution within those tasks. It does not make the original 256 seed files pack more tightly through a different policy.

## 4. Sort compaction algorithm

**Intent:** consolidate files and improve locality for useful predicates.

```text
Read selected rows -> choose range boundaries -> range shuffle -> sort each range -> write ordered files
```

`SparkShufflingDataRewriter` computes an output count from `inputSize * compression-factor`, then requests ordered distribution and a specific number of shuffle partitions. `shuffle-partitions-per-file` can subdivide work; when greater than one, an order-aware coalescer combines adjacent sorted partitions. `SparkSortDataRewriter` supplies the chosen sort order. The write disables applying table distribution/order requirements again because the action has already prepared them. Source: [SparkShufflingDataRewriter.java:123](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkShufflingDataRewriter.java#L123).

Spark range partitioning samples ordering keys to choose boundaries; it does not promise equal compressed bytes per partition. Sorting is per range/rewrite group. Multiple independent groups can retain overlapping key ranges, so this does not create one globally perfect index for the whole table. Iceberg can also add partition requirements when rewriting across spec changes. [Spark 3.5.3 range exchange](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala), [RangePartitioner](https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/Partitioner.scala).

For the measured sort order `(customer_id, id)`, the action requests **9** ranges. Rows can cross old file boundaries during the exchange; they are not constrained to a 28-file whole-input bin. Both engines produced 9 output files. The recorded `spark.sql.shuffle.partitions=8` is only the session default: the action's explicit 9 is visible in both captured plans.

### Lexicographic order and pruning

With `(customer_id, id)`, customer ID is the leading key. Rows for nearby customer IDs tend to occupy nearby regions, which can tighten file and row-group min/max bounds. `id` is ordered within equal customer IDs; it does not get equally strong independent clustering across all customers.

Benefits depend on filter columns, selectivity, groups, statistics modes and the reader. Costs include reading and writing the full selected data, key comparison, range sampling, shuffle serialization/network and possible spills. Even native sort still needs memory and can spill. A faster sort rewrite is separate from a measured improvement in future queries.

## 5. Z-order algorithm

**Intent:** trade a single leading sort key for multidimensional locality.

```text
Project selected values -> order-preserving bytes -> interleave bits -> sort by derived Z value -> drop temporary column -> write
```

For a tiny illustrative two-bit example, `x=10`, `y=01` interleave as `x1 y1 x0 y0 = 1001`. Nearby points often land near each other along the Z curve, but proximity is not perfect and a rectangular predicate can cover multiple disjoint intervals.

Iceberg builds a temporary Z value, applies the same shuffling/sorting machinery, then drops the temporary column. Type conversion, variable-length contribution and maximum interleaved size affect the key. This is not a separate lookup index, does not replace table partitioning, and does not guarantee better pruning for every predicate. Sources: [SparkZOrderDataRewriter.java:128](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderDataRewriter.java#L128), [ZOrderByteUtils.java:34](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/util/ZOrderByteUtils.java#L34).

**Comet evidence boundary:** the inspected `CometIcebergRewriteActionSuite` asserts native scan, write, exchange and sort for a **single-column** `zOrder(id)` case. That does not prove every multicolumn/type combination converts fully. The October 7 benchmark did not run Z-order. Use the actual plan and result checks before proposing a multidimensional native demo. [CometIcebergRewriteActionSuite.scala:73](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:73).

## 6. Delete-aware data rewrite versus delete-file rewrite

### Applying deletes while rewriting data

A position delete identifies a data-file path and row position. An equality delete matches field values under Iceberg's partition and sequence rules. Readers must combine applicable deletes with projected fields and residual semantics before emitting surviving rows. They cannot ignore deletes simply because the output will be a new file.

```text
Old data file + applicable deletes -> surviving rows -> replacement data files -> atomic reference replacement
```

For our delete workload, 16M physical data records minus 800k already-deleted rows gives 15.2M visible rows **both before and after** compaction. The new data files physically contain 15.2M rows. There is no second 5% logical deletion.

The Comet native scan receives Java-planned file tasks, including delete descriptors. Its Rust reader uses iceberg-rust's ArrowReader to apply them. The current scan implementation fills missing delete-file lengths with bounded, deduplicated metadata requests; failure to read a required delete file is a correctness error. The reader enables row selection and adapts returned batches to the requested output schema. Sources: [iceberg_scan.rs:174](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_scan.rs:174), [iceberg_scan.rs:253](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_scan.rs:253), [pipeline.rs:557](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/arrow/reader/pipeline.rs:557).

The local suite exercises both position and equality delete application during a native bin-pack rewrite. The measured benchmark covers position deletes only. [CometIcebergRewriteActionSuite.scala:161](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:161).

### Compacting the delete files themselves

`rewrite_position_delete_files` operates on the position-delete metadata table and replacement **delete files**, not on the table's ordinary payload rows.

```text
Read delete records -> left-semi join live data-file paths -> sort by file_path, pos -> write replacement delete files -> commit
```

The semi-join drops dangling position deletes for data files no longer live in that table state. It is not a logical row DELETE and the code path should not be described as a generic equality-delete consolidation procedure. It can reduce delete-file opens and parsing overhead, while still retaining valid delete records. Source: [SparkBinPackPositionDeletesRewriter.java:76](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackPositionDeletesRewriter.java#L76).

The inspected Comet test asserts **no Comet operators** for this action's staged metadata-table plan. Native delete application during data compaction does not imply a native delete-file writer. [CometIcebergRewriteActionSuite.scala:93](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:93).

Iceberg 1.8.1 also has a separate `remove-dangling-deletes` option on `rewrite_data_files`, default **false**, which runs additional cleanup of dangling delete references after the rewrite. The benchmark did not enable it; 256 position-delete files remain referenced in its recorded output layouts. Removing live metadata references is still distinct from safely deleting physical files retained by older snapshots. [RewriteDataFiles.java:110](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/api/src/main/java/org/apache/iceberg/actions/RewriteDataFiles.java#L110), [RewriteDataFilesSparkAction.java:189](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java#L189).

## 7. Spark JVM: the actual write protocol

Iceberg stages a group's `FileScanTask`s under a UUID. `SparkTableCache` and `ScanTaskSetManager` let the internal Spark read resolve that staged group. After the write completes, `FileRewriteCoordinator` returns its new `DataFile`s to the action; the staged objects are cleared in `finally`. Source: [SparkSizeBasedDataRewriter.java:47](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkSizeBasedDataRewriter.java#L47).

For stock Spark 3.5.3, `V2TableWriteExec.writeWithV2` creates a `DataWriterFactory`, calls `sparkContext.runJob`, receives each `WriterCommitMessage`, calls `onDataWriterCommit`, then calls `BatchWrite.commit(messages)`. A writing task iterates its rows, writes through the data writer, commits the task writer and closes it. Task-write failure calls abort. [Spark write implementation](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala#L364).

```scala
// Teaching pseudocode for the stock V2 write protocol.
factory = batchWrite.createBatchWriterFactory(partitionCount)
messages = runJob(partitions, rows => writeRowsAndCommitTask(factory, rows))
batchWrite.commit(messages)
```

The actual JVM benchmark plan has `BatchScan -> ColumnarToRow -> AppendData` for bin-pack. This is evidence about this plan, not a claim that every JVM Iceberg reader uses a row-only scan. Sorting adds `Exchange` and `Sort` before the row writer.

**Three meanings of commit must remain separate:**

| Commit point | What it means in a compaction group |
| --- | --- |
| `DataWriter.commit()` | Close this task's files and return a commit message |
| `SparkWrite.RewriteFiles.commit(messages)` | Stage the new file descriptors in `FileRewriteCoordinator` |
| `RewriteDataFilesCommitManager.commitFileGroups()` | Replace input file references with new files and commit table metadata |

The internal `AppendData` plan does **not** append a second copy of all rows to the real table. Iceberg supplies a special rewrite `BatchWrite`, whose `commit` stages the result for the outer action. The outer `RewriteFiles` operation removes the selected old file references and adds the replacements. [SparkWrite.java:484](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java#L484), [RewriteDataFilesCommitManager.java:74](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java#L74).

## 8. Comet planning: split, eligibility, native execution

`IcebergWriteStrategy` intercepts eligible Iceberg logical writes and builds a two-operator tree: `IcebergCommitExec` above the write execution. It recognizes append, overwrite variants and copy-on-write replacement shapes. It does not match merge-on-read `WriteDelta`. It keeps the same `BatchWrite` object across the split, avoids duplicating commit during AQE replans, and declines writes requiring the Spark output commit coordinator. Plan-only mode also guards this strategy. [IcebergWriteStrategy.scala:38](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/iceberg/IcebergWriteStrategy.scala:38).

```text
Stock:   child operators -> Spark V2 JVM writer -> BatchWrite.commit
Hybrid:  native child -> row conversion -> IcebergWriteExec JVM writer -> IcebergCommitExec
Native:  native child -> CometIcebergWriteExec Rust writer -> IcebergCommitExec
```

The split operator alone is **not proof of native file production**. The native writer needs its own flag, a fully supported native child region and its writer-compatibility checks. The captured local configuration uses:

```properties
spark.comet.write.iceberg.splitOperator.enabled=true
spark.comet.iceberg.write.enabled=true
spark.comet.scan.icebergNative.enabled=true
spark.comet.explain.planOnly.enabled=false
```

These names belong to the measured development build. Check the version's configuration before copying them into another release. Enabling a flag requests acceleration; the executed plan determines whether it happened.

### Writer compatibility gates

`CometIcebergNativeWrite.checkTriggers` resolves the actual Iceberg write, combines table/write properties and checks the effective contract. Representative boundaries in this checkout:

| Contract | Native requirement / fallback reason |
| --- | --- |
| Table and format | Iceberg v1/v2 and effective Parquet format; v3 writing falls back |
| Child execution | Fully native eligible child; unsupported expressions can break continuity |
| Types | Supported Iceberg/Arrow types; UUID rejected by this writer gate |
| Location and FileIO | Supported schemes, recognized FileIO and default location contract |
| Encoding properties | Vetted Parquet properties and supported compression levels |
| Unsupported behavior | Encryption, bloom-filter enablement, custom layout/provider, unsupported page settings |
| Java compatibility | Executor-side reflection needed to reconstruct commit messages must resolve |

This is a representative map, not an exhaustive stable compatibility list. The gate fails closed when it cannot establish the contract. Planning fallback preserves the JVM path; a native task failure does not imply a general transparent rerun of that task on JVM. Sources: [CometIcebergNativeWrite.scala:133](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeWrite.scala:133), [CometIcebergNativeWrite.scala:198](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeWrite.scala:198).

## 9. Comet Rust: scan, exchange, sort and real Parquet bytes

The JVM scan serializer translates the already-planned Iceberg tasks. Native `IcebergScanExec` passes the task stream to `ArrowReaderBuilder`, with batch size, file concurrency and row selection. Comet is not delegating the whole table scan to the separate `datafusion-iceberg` connector. [CometIcebergNativeScan.scala:1](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeScan.scala:1), [iceberg_scan.rs:215](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_scan.rs:215).

For sorting, `CometExchange` still participates in Spark's shuffle and stage boundaries. `CometSort` sorts batches in the native executor. The cluster scheduler, shuffle dependency and task retries are still Spark concerns; DataFusion is executing within the task, not replacing the distributed Spark application. [CometShuffleExchangeExec.scala:1](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala:1), [native sort planner:1577](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/planner.rs:1577), [DataFusion SortExec:1367](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:1367).

### Native writer construction

The Rust `run_write_task` composes the real writer stack:

```text
RecordBatch stream -> field-ID/schema adaptation -> partition writer -> DataFileWriter
DataFileWriter -> RollingFileWriter -> ParquetWriter -> AsyncArrowWriter -> FileIO output
```

Representative construction from the inspected implementation:

```rust
let parquet_builder = ParquetWriterBuilder::new(writer_properties, iceberg_schema);
let rolling_builder = RollingFileWriterBuilder::new(
    parquet_builder, target_file_size_bytes, file_io,
    location_generator, file_name_generator,
);
let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
```

This abbreviated excerpt omits ownership/cloning details. The important point is that the Rust path performs Parquet encoding, compression, footer generation and file output. It is more than a native scan handing rows to a JVM Parquet writer. [iceberg_write.rs:460](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:460), [parquet_writer.rs:619](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/writer/file_writer/parquet_writer.rs:619).

| Native writer mode | How it handles partitions | Cost / constraint |
| --- | --- | --- |
| Unpartitioned | One rolling writer per task | Task boundaries still limit output consolidation |
| Clustered | Finish a partition before moving to another | Revisiting an already-closed partition is invalid |
| Fanout | Maintain writers for multiple encountered partitions | More simultaneous buffers/files as partition cardinality grows |

Comet adds schema field IDs and handles partition transforms/paths compatible with the Java contract. It names outputs using operation/task attempt identity so retries do not collide. It also paces batches in 1,000-row units to align file-roll checks with Iceberg Java's cadence; this is not a 1,000-row Arrow batch-size setting. The rolling writer checks accumulated size between writes, so the size target is approximate. Sources: [iceberg_write.rs:227](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:227), [iceberg_write.rs:500](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:500), [rolling_writer.rs:160](/Users/srajak/.cargo/git/checkouts/iceberg-rust-1cfaaa0dd97c960f/bb1e4a4/crates/iceberg/src/writer/file_writer/rolling_writer.rs:160).

### Why Java still reads Parquet metadata afterward

The native writer returns a payload containing an encoded data manifest and written file locations. This manifest is a transport representation of task output; it is not independently published as the table's snapshot manifest list.

`CometIcebergWriteExec` decodes it on the executor, rebuilds `DataFile` metrics using the version-matched Iceberg Java Parquet footer logic and `MetricsConfig`, reconciles NaN/bounds information and constructs `SparkWrite.TaskCommit`. These statistics matter to future pruning correctness, so compatibility includes more than returning equal row counts. The driver receives the commit messages, not all output rows. [CometIcebergWriteExec.scala:175](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergWriteExec.scala:175).

`IcebergCommitExec` invokes the captured `BatchWrite.commit`. **For a rewrite group, this stages descriptors in Iceberg's coordinator, just as the JVM path does. The outer action performs final snapshot publication.** Reading the plan label `IcebergCommit` as “the entire compaction's final catalog commit is timed here” would be incorrect. [IcebergCommitExec.scala:67](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/IcebergCommitExec.scala:67), [SparkWrite.java:484](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java#L484).

## 10. Atomicity, conflicts and cleanup

```text
Start snapshot S -> write candidate replacement files -> validate against current state -> publish new metadata
```

Without partial progress, the action waits for the selected groups and performs one replacement commit. With partial progress, batches of completed groups can commit separately, so a failed overall action may already have published successful groups. `max-concurrent-file-group-rewrites` controls concurrent group jobs; it is not the number of executor cores. More group concurrency can increase memory, shuffle pressure and conflict exposure. [RewriteDataFilesSparkAction.java:282](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java#L282).

`RewriteDataFilesCommitManager` calls `table.newRewrite().validateFromSnapshot(startingSnapshotId)`. With the default `use-starting-sequence-number=true`, it assigns the starting snapshot's sequence number to the replacement data. Newer equality deletes can remain applicable to replacement rows. Position deletes reference old paths/positions and need conflict validation; they cannot just be copied onto newly sorted output. [RewriteDataFilesCommitManager.java:82](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java#L82), [MergingSnapshotProducer.java:450](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java#L450).

Concurrent appends can often coexist because they add independent files; overlapping rewrites/removals or new position deletes can invalidate the planned replacement. A metadata commit retry is not always enough: a semantic conflict can require re-planning and re-reading. The catalog's commit protocol decides atomic publication; it is not universally an object-store rename.

| Failure point | Ownership and behavior |
| --- | --- |
| Native stream/write/close failure | Rust abort guard owns files until its result is handed over |
| Manifest decode / JVM metrics rebuild / task-message failure | Executor JVM cleanup owns the newly written files after handoff |
| Group job fails before group commit | Spark/Comet job-abort handling and Iceberg action cleanup coordinate completed outputs |
| Known cleanable outer commit failure | Iceberg `commitOrClean` may remove uncommitted replacement files |
| Unknown outer commit outcome | Do not delete replacements; they may already be referenced by a successful commit |

Sources: [iceberg_write.rs:455](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:455), [CometIcebergWriteExec.scala:185](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergWriteExec.scala:185), [IcebergCommitExec.scala:80](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/IcebergCommitExec.scala:80), [RewriteDataFilesCommitManager.java:112](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java#L112).

Physical old files can remain after a successful rewrite because older snapshots still reference them. Compaction improves the **current** layout; retention/expiration decides when historical files can be reclaimed.

## 11. Code-reading and test map

| Layer | Read in this order | What to inspect |
| --- | --- | --- |
| Iceberg policy | `RewriteDataFilesProcedure` -> `RewriteDataFilesSparkAction` -> `SizeBasedDataRewriter` -> `SizeBasedFileRewriter` | Candidate selection, groups, thresholds, size estimates |
| Iceberg execution | `SparkSizeBasedDataRewriter` -> bin-pack / shuffling / sort / Z-order rewriter -> `SparkWrite.RewriteFiles` | Staged task IDs, distribution, coordinator handoff |
| Spark V2 | `V2TableWriteExec.writeWithV2` -> `WritingSparkTask.run` | Factory, task attempts, commit messages, driver callback |
| Comet planning | `IcebergWriteStrategy` -> `CometIcebergNativeWrite` | Separate split and native gates, fallback reasons |
| Comet task | `CometIcebergNativeScanExec` -> `iceberg_scan.rs` -> `iceberg_write.rs` -> `CometIcebergWriteExec` | Arrow path, files, footer metrics and commit payload |
| Iceberg final commit | `RewriteDataFilesCommitManager` -> `BaseRewriteFiles` -> `MergingSnapshotProducer` | Starting snapshot, sequence numbers, conflicts, cleanup |

Inspected test anchors:

- `CometIcebergRewriteActionSuite`: bin-pack native scan/write, native sort/exchange, single-column Z-order, position/equality delete application, and no native conversion for position-delete-file rewrite. [CometIcebergRewriteActionSuite.scala:48](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:48).
- Iceberg 1.8.1 `TestRewriteDataFilesAction`: high/low delete ratios, bin-pack with deletes, dangling deletes, partial progress and commit/rewrite failures, multiple sort groups and Z-order. [TestRewriteDataFilesAction.java:317](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestRewriteDataFilesAction.java#L317).
- Native writer tests in `iceberg_write.rs`: clustered/fanout modes, rolling cadence across batch boundaries, abort ownership, deterministic file ordering and manifest round trip. [iceberg_write.rs:1898](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/operators/iceberg_write.rs:1898).

**Questions to ask when a new workload falls back:** Is it a real data-table scan? Is the write shape intercepted? Did every child operator convert? Does the effective writer contract pass? Does the internal group plan contain `CometIcebergWrite`? What do the output footers say? A plugin in the Spark Environment tab answers none of those by itself.
