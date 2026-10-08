# CometSort: SQL, Spark plans and the Rust sorter

`CometSort` is the Spark UI name for Comet's replacement of an eligible Spark `SortExec`. It sorts the rows delivered to a task using DataFusion's native external sorter and Arrow arrays. The exchange determines which rows reach each task; sorting establishes their order inside that task. Iceberg still chooses the compaction work and commits the replacement snapshot.

The benefit is the opportunity to keep scan, shuffle, sort and write in native columnar execution. It is not a promise that every sort is faster, that sorting becomes linear, or that native execution removes shuffle and disk I/O.

Read [chapter 21](21-compaction-algorithms-and-execution.md) for Iceberg's compaction policy and writer/commit internals, and [chapter 22](22-table-health-and-compaction-benchmark.md) for the dataset, machine, timings and full demo procedure.

## The complete path

```text
SQL / Iceberg rewrite -> Spark SortExec -> CometSortExec -> protobuf Sort
Task JNI -> Comet PhysicalPlanner -> DataFusion SortExec -> ExternalSorter
Arrow key arrays -> sorted row indices -> gathered output batches -> merge / spill
Ordered batches -> downstream operator or native Iceberg writer -> Java commit
```

**Inspected on 2026-10-08:** Comet checkout `184accac5b9cee6b761a6673c73c263adedef45e`, with the pre-existing local scan/benchmark edits recorded in the source ledger. Rust internals below use the resolved Cargo sources: `datafusion-physical-plan 55.1.0` and `arrow-ord 59.3.0`. The five inspected dependency files match their local cached `.crate` archives byte for byte; this is local source consistency, not proof of the measured binary's source. Iceberg SQL examples match runtime 1.8.1 and the baseline Spark runtime is 3.5.3. No benchmark or engine test was rerun for this chapter.

## 1. Start from SQL and the captured plan

This is the actual sorted-compaction statement from the 16M-row benchmark:

```sql
-- Scan + range exchange + sort + replacement-file write + snapshot commit.
CALL local.system.rewrite_data_files(
  table => 'compaction_bench.clean',
  strategy => 'sort',
  sort_order => 'customer_id ASC NULLS FIRST, id ASC NULLS FIRST',
  options => map(
    'rewrite-all', 'true',
    'target-file-size-bytes', '134217728',
    'max-concurrent-file-group-rewrites', '1',
    'partial-progress.enabled', 'false'
  )
);
```

The target is **128 MiB per file**, not 128 MiB of sort memory. `max-concurrent-file-group-rewrites=1` serializes rewrite groups; it does not limit the job to one task. The saved exchange has nine partitions. That is an observed planning result for this fixture, not a fixed Comet setting.

The [saved JVM plan](assets/compaction/jvm-sort.plan.txt) and [saved Comet plan](assets/compaction/comet-sort.plan.txt), simplified only by removing attributes and IDs:

```text
JVM                                      Comet
AppendData                               IcebergCommit
  Sort(customer_id, id), global=false       CometIcebergWrite
    Exchange(range, 9 partitions)             CometSort(customer_id, id)
      ColumnarToRow                             CometExchange(range, 9, CometNativeShuffle)
        BatchScan                                CometIcebergNativeScan
```

Read from the bottom up. In the Comet plan, the native scan supplies batches, range exchange routes rows, `CometSort` orders them, and the native writer produces Parquet. The `IcebergCommit` node coordinates the write result with Java; the outer Iceberg rewrite action performs the final replacement-snapshot commit described in chapter 21.

The JVM plan's `global=false` does **not** mean rows are sorted without a useful layout. Iceberg explicitly requested the range exchange first, followed by a local sort. Distribution and ordering are separate requirements.

### Three SQL shapes to distinguish

```sql
-- Local ordering only; partitions need not have disjoint key ranges.
EXPLAIN FORMATTED
SELECT id, customer_id, amount
FROM local.compaction_bench.clean
SORT BY customer_id ASC NULLS FIRST, id ASC NULLS FIRST;

-- Global result ordering; Spark must also satisfy ordered distribution.
EXPLAIN FORMATTED
SELECT id, customer_id, amount
FROM local.compaction_bench.clean
ORDER BY customer_id ASC NULLS FIRST, id ASC NULLS FIRST;

-- Top-K: may select a different operator and algorithm.
EXPLAIN FORMATTED
SELECT id, customer_id, amount
FROM local.compaction_bench.clean
ORDER BY customer_id ASC NULLS FIRST, id ASC NULLS FIRST
LIMIT 10;
```

`EXPLAIN` plans these queries; remove it to execute them. `ORDER BY ... LIMIT` can become `CometTakeOrderedAndProject`, with local and final bounded ordering work, instead of a full `CometSort`. It is therefore a poor substitute for measuring sorted compaction. Source: [CometTakeOrderedAndProjectExec.scala:45](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometTakeOrderedAndProjectExec.scala:45).

In Spark 3.5.3, a global `SortExec` requests `OrderedDistribution`; local sorting retains the child's partitioning. The JVM implementation creates an `UnsafeExternalRowSorter`, including a prefix comparator and a restricted single-key radix option. Spark already has code generation, compact rows, memory management and spilling. The comparison is not native code versus an unoptimized object-per-row sorter. Source: [Spark 3.5.3 SortExec.scala](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/SortExec.scala).

## 2. Scala: replace the operator and preserve its contract

[CometExecRule.scala:90](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala:90) registers `classOf[SortExec] -> CometSortExec`. The serializer checks type/expression support and requires a convertible child. This excerpt is from [operators.scala:1447](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala:1447):

```scala
val sortOrders = op.sortOrder.map(exprToProto(_, op.child.output))

if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
  val sortBuilder = OperatorOuterClass.Sort
    .newBuilder()
    .addAllSortOrders(sortOrders.map(_.get).asJava)
  Some(builder.setSort(sortBuilder).build())
}
```

The omitted branch records a fallback reason and returns `None`. `createExec` builds `CometSortExec`, retaining Spark's output attributes, ordering and original plan. One line establishes a critical boundary:

```scala
override def outputPartitioning: Partitioning = child.outputPartitioning
```

Source: [operators.scala:1482](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala:1482). `CometSort` does not itself change Spark partition ownership or perform a cluster-wide merge. Spark's exchanges establish that ownership; native sort execution happens inside the scheduled task.

### What crosses the language boundary?

The operator carries expressions, not SQL text to reparse. Each sort key records its bound child expression, ascending/descending direction and null placement. [CometSortOrder.scala:54](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/CometSortOrder.scala:54) maps direction to `0/1` and null ordering to `0/1`. The protobuf definitions are:

```protobuf
message Sort {
  repeated spark.spark_expression.Expr sort_orders = 1;
  optional int32 fetch = 3;
  optional int32 skip = 4;
}

message SortOrder {
  Expr child = 1;
  SortDirection direction = 2;
  NullOrdering null_ordering = 3;
}
```

Sources: [operator.proto:805](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/proto/src/proto/operator.proto:805), [expr.proto:457](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/proto/src/proto/expr.proto:457). Ordinary `CometSortExec.convert` sets the keys; `fetch`/`skip` also allow other native planning paths to express bounded ordering.

`CometExecIterator` creates a native task context with the serialized plan, inputs, configuration, metrics, memory manager and local disk directories. `Native_executePlan` lazily calls `PhysicalPlanner.create_plan` when execution first starts. This is a task/stream boundary, not a JNI invocation for every comparison. Sources: [CometExecIterator.scala:124](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometExecIterator.scala:124), [jni_api.rs:1090](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/jni_api.rs:1090).

## 3. Rust: Comet constructs DataFusion SortExec

[PhysicalPlanner.create_sort_expr](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/planner.rs:1015) creates a `PhysicalSortExpr`: its expression reads the key column or computes the key, and its `SortOptions` carry direction/null semantics. The `OpStruct::Sort` branch creates the actual native operator:

```rust
let fetch = sort.fetch.map(|num| num as usize);

let mut sort_exec: Arc<dyn ExecutionPlan> = Arc::new(
    SortExec::new(
        LexOrdering::new(exprs?).unwrap(),
        Arc::clone(&child.native_plan),
    )
    .with_fetch(fetch),
);
```

Source: [planner.rs:1577](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/planner.rs:1577). This `SortExec` comes from DataFusion. Optional offset handling wraps it with `GlobalLimitExec`; ordinary sorted compaction has no fetch/offset.

DataFusion's constructor describes a single sorted output partition. That is a property of the **native plan inside a Spark task**. It does not imply one Spark output partition for the whole nine-task exchange.

**A misleading filename:** Comet's [native/core/src/execution/sort.rs:23](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/sort.rs:23) is a radix helper for packed shuffle row addresses/partition IDs. It is not the implementation of SQL `CometSort`. Follow `planner.rs` into DataFusion's `sorts/sort.rs` instead.

### Execute chooses among three paths

In [DataFusion SortExec::execute:1367](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:1367):

| Condition | Behavior |
| --- | --- |
| Input properties already satisfy the ordering | Return the input, optionally capped by a limit |
| Ordering unsatisfied and `fetch` is present | Build `TopK`, insert batches and emit bounded results |
| Ordering unsatisfied and no `fetch` | Build `ExternalSorter`, insert every batch, then finish the sort |

The full-sort loop is:

```rust
while let Some(batch) = input.next().await {
    let batch = batch?;
    sorter.insert_batch(batch).await?;
}
drop(input);
sorter.sort().await
```

Source: [sort.rs:1444](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:1444). Full sort is blocking with respect to its unsorted task input: it cannot emit the final globally ordered task result just because the first batch has been sorted. Another batch might contain a smaller key.

## 4. Arrow: compare keys, sort indices, gather rows

The normal external-sort run path in this dependency is:

```text
ExternalSorter.in_mem_sort_stream -> sort_batch_stream -> sort_batch_chunked
IncrementalSortIterator.next -> evaluate_to_sort_column -> lexsort_to_indices
Sorted UInt32 indices -> take_record_batch -> ordered output chunks
```

Sources: [sort.rs:733](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:733), [sort.rs:919](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:919), [stream.rs:334](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/stream.rs:334).

Teaching pseudocode, preserving those calls but omitting error handling and iterator state:

```rust
let keys = expressions.iter()
    .map(|expr| expr.evaluate_to_sort_column(&batch))
    .collect::<Result<Vec<_>>>()?;
let indices = lexsort_to_indices(&keys, None)?;
let chunk_indices = indices.slice(cursor, batch_size);
let output = take_record_batch(&batch, &chunk_indices)?;
```

The nearby `sort_batch` helper expresses the same index-then-gather idea using `take_arrays`, but the inspected `ExternalSorter` calls the chunked iterator path above. Also, `sort_batch_chunked` collects those chunks into a vector in this version. Chunked output does not prove that only one output chunk is ever live during a sort call.

### A four-row example

Illustrative input, ordered by `customer_id ASC NULLS FIRST, id ASC NULLS FIRST`:

| Original row index | customer_id | id | amount |
| --- | --- | --- | --- |
| 0 | 20 | 7 | 3.5 |
| 1 | 10 | 9 | 8.0 |
| 2 | 20 | 2 | 1.0 |
| 3 | NULL | 4 | 6.0 |

```text
Compare customer_id -> if equal compare id -> ordered indices [3, 1, 2, 0]
Gather every output column with [3, 1, 2, 0] -> rows stay aligned
```

Sorting only the customer column independently would corrupt the records. Indices describe a shared permutation applied to all payload columns, including `amount`, `status` and `payload` in the real fixture.

[Arrow lexsort_to_indices:940](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/arrow-ord-59.3.0/src/sort.rs:940) dispatches a supported single key to `sort_to_indices`. Multiple keys use lexicographic comparison, including specialized fixed-key-count comparators for two through five columns. For the benchmark's two keys and no limit, it sorts the row-index vector using the two-key comparator. This is ordinary comparison sorting, not the shuffle radix helper.

The general [LexicographicalComparator::compare:1135](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/arrow-ord-59.3.0/src/sort.rs:1135) shows the semantics:

```rust
for comparator in &self.compare_items {
    match comparator(a_idx, b_idx) {
        Ordering::Equal => continue,
        r => return r,
    }
}
Ordering::Equal
```

Comparisons stop at the first differing key. Null placement and descending order are part of each comparator. Equal keys have no stable input-order guarantee; add a unique tie-breaker when a reproducible total order matters. The fixture uses `id` after `customer_id`.

Gathering rows is not generally zero-copy. It allocates/reorders output buffers, and wide rows can make memory bandwidth significant even when key comparisons are cheap. Columnar batch execution also does not prove that every comparison is SIMD. See [Arrow memory](13-arrow-memory-and-kernels.md) and [hardware](14-vectorization-and-hardware.md).

### Spark floating-point semantics still matter

The inspected Comet planner normalizes scalar floating-point **comparison keys** so NaN payloads and signed zero compare according to Spark's ordering rules. Returned values retain their original representations. Nested floating-point values inside arrays/structs/maps have separate compatibility restrictions; a scalar fix does not establish nested compatibility. Sources: [CometSortOrder.scala:38](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/serde/CometSortOrder.scala:38), [planner.rs:984](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/native/core/src/execution/planner.rs:984).

## 5. Larger than memory: runs, spill and merge

`ExternalSorter.insert_batch` reserves merge headroom, reserves memory for the incoming batch and buffers it. If growing the reservation fails while buffered batches exist, it sorts/spills that buffered data, then retries the reservation. A batch that still cannot fit can produce a resource-exhaustion error. Spill support is not unlimited memory. Sources: [insert_batch:321](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:321), [reserve_memory_for_batch_and_maybe_spill:799](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:799).

For small buffered input, `in_mem_sort_stream` may concatenate batches and sort one larger batch. For larger input, it forms sorted runs and merges them. The inspected source also coalesces eligible single-key runs to reduce merge fan-in; this is a version-specific detail, not a universal property of every Comet build. Source: [in_mem_sort_stream:595](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/sort.rs:595).

```text
Incoming batches -> memory reservation -> sorted runs in memory
Memory pressure -> sorted spill files -> release buffered state -> accept more input
End of input -> merge remaining runs / spill files -> ordered output batches
```

[StreamingMergeBuilder::build:180](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/streaming_merge.rs:180) selects specialized single-key cursors where possible; general multi-key merge uses `RowCursorStream`. Spill files use a multi-level merge builder. [SortPreservingMergeStream](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/merge.rs:54) keeps a **loser tree**: a tournament structure that remembers losing run heads so it can efficiently find the next smallest row after advancing the winner.

Illustrative merge: runs `[1, 4, 8]`, `[2, 3, 9]`, `[0, 7]` emit `0, 1, 2, 3, 4, 7, 8, 9`. The merge does not sort their concatenation from scratch. Comparison-based run sorting is roughly `O(n log n)`; merging `R` runs is roughly `O(N log R)`, before key-comparison, copying and I/O costs. Multi-level spills add passes and disk traffic.

Sort memory includes decoded input, indices, output buffers, comparison/merge keys and reserved merge headroom. It cannot be estimated from compressed Parquet bytes alone. Increasing task concurrency can increase total memory pressure even if each task processes less data. Spark task spill counters and native operator spill metrics can have different accounting scopes; do not assume they are interchangeable.

## 6. Where the benefit comes from

| Mechanism | Why it can help | What must still be measured |
| --- | --- | --- |
| Native columnar continuity | Avoids the saved JVM plan's columnar-to-row path and keeps adjacent eligible operators working on batches | Actual executed plan and boundary conversions |
| Native key comparison and gathering | Works directly on Arrow buffers and uses type/key-count-specific kernels | CPU profiles, row width, key types and memory bandwidth |
| Native shuffle + sort + write together | Can reduce conversion/serialization overhead across the rewrite pipeline | End-to-end job time, shuffle bytes, write throughput and commit time |
| External-sort memory accounting | Allows bounded runs and spill instead of requiring all rows in memory | Spill count/bytes, disk throughput and failure behavior |
| Already-ordered / bounded paths | Can avoid unnecessary full sorting where the plan proves it safe | Ordering properties and selected Top-K/full-sort operator |

These are mechanisms or performance hypotheses, not measured attribution. Tiny inputs can be dominated by planning/native setup. Wide strings, nested comparisons, skewed range partitions, frequent spilling, slow storage and fallback conversions can reduce or reverse a gain. Both Spark and DataFusion already implement optimized external sorting.

### What our benchmark establishes

| Sorted compaction | Spark JVM | Comet |
| --- | --- | --- |
| Median complete rewrite | 9.598 s | 4.156 s |
| Active input data files | 256 | 256 |
| Active output data files | 9 | 9 |
| Logical row count | 16,000,000 | 16,000,000 |

The ratio is **2.31x for the whole sorted compaction**, including scan, exchange, sort, write and the procedure's final commit. It is not a `CometSort` microbenchmark. The fixture starts with about 1.18 GiB of Parquet; five measured trials per engine/workload and the checks are preserved in [the benchmark evidence](assets/compaction/compaction-16m.json). Dataset/machine/cache/configuration qualifications are in chapter 22.

Disabling only `spark.comet.exec.sort.enabled` during this compaction is not automatically a clean sort-only experiment. In this checkout, the native Iceberg writer requires an eligible native child; losing native sort can also change the writer or insert conversions. Inspect both plans before attributing a timing difference to one operator.

### How this improves table health

Compaction reduces active file fragmentation. Sorting additionally clusters nearby key values, which can tighten file and row-group min/max ranges and improve pruning for matching predicates. Both engines can produce that layout. Comet's demonstrated benefit here is spending less elapsed time performing the rewrite, not an exclusive ability to create better statistics.

For `(customer_id, id)`, predicates on the leading customer key are natural candidates for better pruning. Predicates only on `id` need not benefit as much. Independent rewrite groups can still have overlapping key ranges; this is not a global table index. Neither sorted files nor a table sort-order declaration guarantees that a later `SELECT` without `ORDER BY` returns sorted results. Post-compaction read latency and scan-byte savings were not measured in this benchmark.

## 7. Demo: make the work visible in Spark History Server

1. Use the paired fixture and fresh JVM/Comet sessions from chapter 22. Restore the same starting file manifest before each rewrite; running the second engine on the first engine's compacted output changes the work.
2. Show the SQL above and the before/after file-health queries. State the input: 16M rows, 256 files, 128 MiB target, two sort keys.
3. In the SQL tab, find the rewrite's internal scan/exchange/sort/write execution as well as the outer procedure. A `CALL` can launch multiple internal executions; explaining only the command does not reveal all of them.
4. Compare the executed plans with the saved pair. Point to `ColumnarToRow` / `Sort` / `AppendData`, then native scan / `CometExchange` / `CometSort` / native write. Check the final adaptive plan where AQE is enabled.
5. Open the `CometSort` node and compare task distributions and spill metrics. Check shuffle read/write and slowest tasks at the stage level. The operator exposes `spill_count` and `spilled_bytes`, displayed as **number of spills** and **total spilled bytes**.
6. Show the final file count and logical-row checks before the median timing comparison. Explain that the output layout is comparable while the execution path changes.

The metrics are defined in [operators.scala:1503](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala:1503). A missing metric is not proof of zero spill. Operator times aggregated across tasks, task elapsed times and job wall time are different quantities; summing SQL-node times does not reconstruct wall time.

For a rehearsed run with native-plan logging, set these before executing the query in the configured Comet session:

```sql
SET spark.comet.exec.sort.enabled = true;
SET spark.comet.explain.native.enabled = true;
```

The first switch permits native sort but does not override unsupported expressions/types or missing Comet session setup. The second logs the native tree before execution and again with metrics; inspect executor logs as appropriate, since this is task execution. Sources: [CometConf.scala:222](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometConf.scala:222), [CometConf.scala:819](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/main/scala/org/apache/comet/CometConf.scala:819). Logging is diagnostic overhead; keep configuration comparable in timed runs.

To study sorting separately later, hold input/layout, partitioning, keys, output columns, sink and memory budget constant; consume every result without collecting millions of rows on the driver. Use repeated trials and capture CPU, native spills and shuffle metrics. A `COUNT(*)` wrapper may eliminate ordering, and adding `LIMIT` may switch to Top-K, so inspect the executed plan. This is a proposed experiment, not a result recorded here.

## 8. Source-inspected tests and reading order

| Source | What to learn |
| --- | --- |
| [CometExecSuite.scala:2207](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala:2207) | Native sort retains child output partitioning |
| [CometExecSuite.scala:1943](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala:1943) | Type-dependent sort support and fallback cases |
| [CometExpressionSuite.scala:147](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometExpressionSuite.scala:147) | Strict scalar floating-point ordering and preservation of returned values |
| [CometIcebergRewriteActionSuite.scala:59](/Users/srajak/Documents/repos/oss/apache/datafusion-comet/spark/src/test/scala/org/apache/comet/CometIcebergRewriteActionSuite.scala:59) | Sorted rewrite asserts native scan, write, exchange and sort, plus data-order checks |
| [DataFusion stream.rs:410](/Users/srajak/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-physical-plan-55.1.0/src/sorts/stream.rs:410) | Chunked gathering and output buffer behavior |

These are inspected test assertions, not passing test runs from this update. For a first source walkthrough, read the saved Comet plan, `CometSortExec.convert`, `PhysicalPlanner`'s sort branch, `SortExec::execute`, `IncrementalSortIterator::next`, then `StreamingMergeBuilder` and `SortPreservingMergeStream`.

```text
Speaker cue: exchange assigns ranges -> CometSort orders task rows -> writer creates sorted files
Performance cue: same maintenance goal -> eligible native pipeline -> measured whole-job gain
Health cue: fewer files + useful clustering -> possible cheaper reads -> verify with read measurements
```
