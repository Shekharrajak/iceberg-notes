# Table health, maintenance SQL and the compaction benchmark

A healthy table has manageable files and metadata, affordable delete application, useful clustering, and retention that matches its readers. Compaction addresses only some of these. Comet can reduce eligible rewrite execution cost; it does not choose the retention policy or make every maintenance operation native.

Start with [compaction algorithms and execution internals](21-compaction-algorithms-and-execution.md) for the source trace. This chapter connects the mechanisms to table health, the October 7 measurements and an audience demo.

## 1. Which operation fixes which problem?

| Symptom | Operation | Physical algorithm | Expected effect and limit |
| --- | --- | --- | --- |
| Too many small data files | Bin-pack `rewrite_data_files` | Read visible rows, combine task work, write larger files | Fewer file opens/descriptors; no deliberate new clustering |
| Poor selective-filter locality | Sort or Z-order data rewrite | Compute ordering keys, exchange, sort, write | Can improve file/row-group pruning; pays extra shuffle and sort cost |
| Many deleted rows still physically present | Delete-aware data rewrite | Apply deletes while scanning, write survivors | Less dead data in replacement files; visible rows unchanged |
| Many small position-delete files | `rewrite_position_delete_files` | Read delete records, retain live paths, sort, rewrite | Fewer delete-file opens; not a generic equality-delete rewrite |
| Many fragmented manifests | `rewrite_manifests` | Reorganize metadata entries, group by partition context, write replacement manifests | Can reduce planning overhead; no payload row compaction |
| Too much retained snapshot history | `expire_snapshots` | Apply retention, update metadata, identify files no longer referenced by retained state | Reclaims eligible historical files; time travel changes |
| Files never committed or left by failed work | `remove_orphan_files` | List candidate files and compare against reachable table references | Reclaims unreferenced storage; does not improve clustering |
| Too many old metadata JSON versions | Metadata retention properties | Delete obsolete metadata versions according to policy | Controls metadata-file accumulation; distinct from data compaction |

**Comet scope:** the benchmark establishes acceleration for three `rewrite_data_files` shapes with the eligible native writer. The inspected test deliberately keeps position-delete-file rewriting on JVM. Metadata maintenance may execute Spark subplans, but no measured acceleration for those actions is claimed here. “All maintenance is metadata-only” and “Comet accelerates every maintenance action” are both inaccurate.

Source-backed operations: [RewriteManifestsSparkAction.java:79](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteManifestsSparkAction.java#L79), [ExpireSnapshotsSparkAction.java:1](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java#L1), [DeleteOrphanFilesSparkAction.java:83](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L83). See also the versioned [Iceberg 1.8.1 maintenance guide](https://iceberg.apache.org/docs/1.8.1/maintenance/).

## 2. Table health has several dimensions

### Data-file health

Measure file counts, total bytes, size percentiles, small-file fraction, and those same measures **per partition/spec**. An average can hide thousands of tiny files alongside a few huge ones. Many tiny files add listing/metadata descriptors, footer requests, reader setup and scheduling overhead. Very large files can reduce useful concurrency or enlarge task work; Parquet splits mean file count is not an exact task count.

A useful diagnostic formula is:

```text
small_file_fraction = count(data files below chosen threshold) / count(data files)
mean_file_bytes = sum(data-file bytes) / count(data files)
```

Choose the threshold relative to the workload and target size. A tiny historical partition may already be as compact as possible. Repeatedly rewriting it cannot manufacture enough rows to fill a 128 MiB file.

### Delete health

Track number/bytes of position and equality delete files, their applicability to scanned data, and reader delete-application cost. A raw delete-file/data-file count ratio is a pressure signal, not a percentage of rows deleted. One delete file can apply to many data files; many records can refer to the same deleted row.

Distinguish these quantities:

| Metric | Meaning |
| --- | --- |
| `SUM(record_count)` for `content=0` | Physical records described by current data files, before logical delete application |
| `SUM(record_count)` for `content=1/2` | Delete records; not necessarily distinct deleted rows |
| `SELECT COUNT(*) FROM table` | Visible logical rows under the snapshot's delete semantics |
| Delete files opened / bytes read | Actual reader work for a query, subject to engine metric coverage |

The benchmark's 5% density is known from its deterministic generator, not inferred from the metadata file-count ratio.

### Clustering health

Check whether relevant predicates overlap many file bounds and whether the reader skips files/row groups. Good sorting can tighten bounds; broader groups, wrong leading columns, truncation of string statistics or unsuitable predicates can limit the benefit. A bin-pack can even broaden per-file ranges by combining unrelated inputs. Fewer files and better pruning are different properties.

Use a fixed read-query suite before and after rewriting. Include selective range/point predicates and a full-scan aggregation. Report returned results, scanned bytes, files/row groups where available, and latency. Do not substitute a rewrite speedup for a read-query speedup.

### Metadata and retention health

A current snapshot references a manifest list; manifests describe data/delete files. Rewrites create new state while older retained snapshots can keep old files reachable. Branches/tags and active readers influence safe retention. Storage can temporarily grow during a rewrite because old inputs, new outputs and shuffle/spill coexist.

```text
New compacted snapshot -> old snapshots still reference old files -> retention permits expiration -> eligible old objects removed
```

The current `files` table is not a complete inventory of retained storage. Conversely, summing file lists across every snapshot can double-count shared objects. Count distinct physical paths when measuring retained bytes.

## 3. Metadata-maintenance algorithms

### Rewrite manifests

The action reads manifest entries, groups/reorders metadata around partition information, writes replacement manifests and commits the new manifest arrangement. It does not decode all payload Parquet rows or change the data files' row order. It is useful when manifest fragmentation or poor metadata grouping increases planning work. Measure driver planning latency, manifests considered/read, manifest count/bytes and commit cost; a lower manifest count alone is not a measured query gain. [RewriteManifestsSparkAction.java:79](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteManifestsSparkAction.java#L79).

### Expire snapshots

The algorithm identifies snapshots eligible for removal under the requested age/count and reference-retention rules, updates table metadata, then computes/deletes files no longer required by retained state. A file used by both an expired and retained snapshot must survive. It is reference-aware garbage collection, not “delete everything older than timestamp” at the object-store level. [RemoveSnapshots.java:1](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/core/src/main/java/org/apache/iceberg/RemoveSnapshots.java#L1), [ExpireSnapshotsSparkAction.java:1](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java#L1).

Compaction alone does not free the old files. Snapshot expiration alone does not consolidate the current data files. They solve different problems and may be scheduled at different cadences.

### Remove orphan files

The action compares a storage listing or supplied file list against paths reachable through table metadata. Candidate age and location restrictions protect files that writers may still be producing. Path/scheme/authority normalization matters: mismatched path representations can make a referenced object appear unreferenced. Start with a dry run and a retention window that exceeds possible in-flight writes and recovery delays. This is part of the deletion algorithm's correctness, not a performance knob to set near zero for a demo. [DeleteOrphanFilesSparkAction.java:83](https://github.com/apache/iceberg/blob/apache-iceberg-1.8.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L83).

Orphan deletion is also different from expiring snapshots: a file can be old and still correctly referenced, or recent and not yet committed. In catalogs/tables that share files, reachability and garbage-collection policy need particular care.

### Old metadata JSON versions

`write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max` control cleanup of previous metadata versions tracked by the table. They do not replace snapshot retention, orphan cleanup or data-file rewrite. Use the catalog/table retention contract rather than deleting metadata JSONs by filename. [Iceberg maintenance reference](https://iceberg.apache.org/docs/1.8.1/maintenance/#remove-old-metadata-files).

## 4. The measured JVM versus Comet result

**Observed:** local run on 2026-10-07, 5 measurements per engine/workload, medians below. This replaces the older tiny-fixture result for slides 12 and 13; the [earlier chapter 18 evidence](18-iceberg-tests-and-benchmark-evidence.md) remains historical.

| Rewrite | Spark JVM median | Comet median | JVM / Comet | Data files, both engines |
| --- | ---: | ---: | ---: | --- |
| Bin-pack | 3.651 s | 1.445 s | 2.53x | 256 -> 10 |
| Bin-pack + position deletes | 4.295 s | 1.509 s | 2.85x | 256 -> 10 |
| Sort by customer_id, id | 9.598 s | 4.156 s | 2.31x | 256 -> 9 |

Ratios use unrounded medians. Fewer data files show layout improvement, while matching checks show that the visible rows were preserved. The experiment did not measure post-compaction analytical-query latency. The suite restored the original seed after each trial, so the current live tables can show 256 files again. The saved output layouts prove the 10/9-file results; for a live before/after inspection, inspect the rewritten snapshot before the harness restores it.

[Download the complete curated evidence](assets/compaction/compaction-16m.json): all timing samples, 60 correctness records, original input-manifest hashes, software binary hashes, dataset/method and six captured median-trial plans. The source run was `/tmp/comet-compaction-benchmark/run-20261007-16m`; the copy in this notebook makes the curated evidence independent of that temporary directory. Local warehouse paths in captured plans are shortened; operators are unchanged.

### Dataset and machine

| Item | Recorded value |
| --- | --- |
| Dataset | Deterministic synthetic events; not TPC-H/TPC-DS |
| Schema | id BIGINT, customer_id BIGINT, event_day DATE, amount DECIMAL(12,2), status STRING, payload STRING |
| Input | 16,000,000 physical rows; 256 data files; 1,267,756,086 data bytes = 1.18 GiB |
| Fragmentation | 4.72 MiB mean file size; 62,500 rows per seed file |
| Table | Unpartitioned Iceberg v2, Snappy Parquet, target 128 MiB |
| Delete case | Five MOR DELETE batches; 800,000 rows deleted; 15.2M visible rows; 256 position-delete files |
| Machine | Mac16,7, 14 logical CPUs, 48 GiB RAM, local filesystem |
| Spark resources | local[8], 4 GiB driver JVM heap, 8 GiB configured off-heap in both arms |
| Runtime | Spark 3.5.3, Iceberg 1.8.1, local Comet 1.1.0-SNAPSHOT, Java 17.0.18 (Amazon) |
| Execution settings | AQE off; SQL shuffle default 8; sort action explicitly requests 9 |
| Storage/cache | Warm filesystem cache from a JVM validation scan; no Spark persisted data |

The payload is a SHA-256 hex string to avoid a trivially compressible all-constant dataset. Customer IDs have one million possible values, dates span 90 days and status is null every eighth row. This is a controlled small-file workload, not evidence about production partition skew or S3 latency.

### Measurement method

```text
Restore seed snapshot -> verify file manifest and rows -> warm-up rewrite -> restore seed -> measured rewrite -> verify output
```

Five paired rounds alternate JVM-first and Comet-first order. Engines run in separate Spark applications, ten applications total. Each application runs bin-pack, delete-aware bin-pack and sort; each case gets a full warm-up immediately before its measured trial. There are 30 measured rewrites plus 30 warm-ups.

The timer surrounds `spark.sql(CALL ...).collect()` and includes procedure planning, group execution and the final Iceberg snapshot commit. It excludes Spark startup, data preparation, validation, plan export and restoring/cleaning the trial snapshot. Validation scans warm the filesystem cache; call this a warm-cache comparison.

Every run checks the original seed file-manifest hash. Before/after logical checks use row count, `SUM(amount)`, and two order-independent aggregate `xxhash64` sums over all six columns, summed as DECIMAL(38,0). All 60 matched. Aggregate hashes are strong regression checks, not a mathematical proof against every possible collision. Native output Parquet footers identify `Apache Iceberg 1.8.1 (Comet)`.

### All measured timing samples

| Case | Engine | Round 1 | Round 2 | Round 3 | Round 4 | Round 5 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Bin-pack | JVM | 4.079 | 3.877 | 3.615 | 3.651 | 3.574 |
| Bin-pack | Comet | 1.445 | 1.659 | 1.443 | 1.338 | 1.448 |
| With deletes | JVM | 4.438 | 4.295 | 3.982 | 4.211 | 4.600 |
| With deletes | Comet | 1.930 | 1.809 | 1.353 | 1.509 | 1.273 |
| Sort | JVM | 10.099 | 8.772 | 9.701 | 7.817 | 9.598 |
| Sort | Comet | 5.363 | 4.817 | 3.620 | 4.156 | 3.338 |

Five samples expose some variability; they do not establish a fleet-wide confidence interval. The two-arm result combines native scan/operator/write effects. To isolate native writer contribution, add a third arm with native child execution and a verified JVM writer.

## 5. Executable SQL for the benchmark cases

Use an isolated demo namespace. These calls rewrite the selected table's files. They are the saved benchmark procedures, not commands executed while authoring these notes. Session/JAR/catalog configuration must match the chosen engine. The live benchmark harness restored the exact seed snapshot between calls; running the following repeatedly against the already-compacted table is not an equivalent benchmark.

### Clean bin-pack

```sql
-- Scan + write; no exchange or sort in the captured plan.
CALL local.system.rewrite_data_files(
  table => 'compaction_bench.clean',
  strategy => 'binpack',
  options => map(
    'rewrite-all', 'true',
    'target-file-size-bytes', '134217728',
    'max-concurrent-file-group-rewrites', '1',
    'partial-progress.enabled', 'false'
  )
);
```

### Bin-pack with position deletes

```sql
-- Scan + delete application + write; preserve the 15.2M already-visible rows.
CALL local.system.rewrite_data_files(
  table => 'compaction_bench.deletes',
  strategy => 'binpack',
  options => map(
    'rewrite-all', 'true',
    'target-file-size-bytes', '134217728',
    'max-concurrent-file-group-rewrites', '1',
    'partial-progress.enabled', 'false'
  )
);
```

### Sort compaction

```sql
-- Scan + range exchange + sort + write.
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

[Saved bin-pack SQL](assets/compaction/binpack.sql), [delete-aware SQL](assets/compaction/binpack_deletes.sql), [sort SQL](assets/compaction/sort.sql). A selective `where` can restrict production maintenance to candidate files in colder partitions. It does not filter away nonmatching rows inside selected files; see chapter 21.

## 6. How the dataset was fragmented

The following is a standalone clean-table seed example matching the generator and table properties. Use a fresh namespace/table for each new fixture; it is not an instruction to overwrite an existing table.

```sql
CREATE NAMESPACE IF NOT EXISTS local.compaction_bench;

CREATE TABLE local.compaction_bench.clean (
  id BIGINT,
  customer_id BIGINT,
  event_day DATE,
  amount DECIMAL(12,2),
  status STRING,
  payload STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.distribution-mode' = 'none',
  'write.target-file-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'snappy'
);

INSERT INTO local.compaction_bench.clean
SELECT id,
  pmod(xxhash64(id), 1000000),
  date_add(DATE '2025-01-01', cast(pmod(id,90) AS int)),
  cast(pmod(id*37,100000)/100.0 AS decimal(12,2)),
  CASE WHEN pmod(id,8)=0 THEN NULL
    ELSE concat('status_', cast(pmod(id,5) AS string)) END,
  sha2(cast(id AS string),256)
FROM range(0,16000000,1,256);
```

For the delete fixture, create another table named `deletes` with the same schema/properties and run the same generator into it. Then execute five separate DELETE statements, with predicates `pmod(id,100)=0`, `=1`, `=2`, `=3`, `=4`. Five statements reproduce the mutation pattern; a single `IN` predicate has equivalent visible rows but need not produce the same delete-file layout. Inspect the resulting `files` table rather than assuming “five batches means 5 x 256 delete files.” The observed seed has 256 position-delete files.

The `range` has 256 input partitions and distribution is disabled for seeding. The resulting fragmentation was verified through metadata; input partition count alone does not universally guarantee a file count under every writer, AQE configuration or dataset size.

## 7. Read-only health SQL

These are Spark SQL inspection queries for the example Iceberg catalog/table. Change the identifier for a partitioned production table. No cleanup is performed.

```sql
-- Current file counts and sizes. content: 0=data, 1=position delete, 2=equality delete.
SELECT content,
       count(*) AS files,
       sum(record_count) AS physical_records,
       round(sum(file_size_in_bytes) / 1048576.0, 2) AS total_mib,
       round(avg(file_size_in_bytes) / 1048576.0, 2) AS mean_mib,
       round(min(file_size_in_bytes) / 1048576.0, 2) AS min_mib,
       round(max(file_size_in_bytes) / 1048576.0, 2) AS max_mib
FROM local.compaction_bench.clean.files
GROUP BY content
ORDER BY content;

-- Distribution matters more than an average alone.
SELECT percentile_approx(file_size_in_bytes, array(0.1, 0.5, 0.9)) AS size_percentiles,
       sum(CASE WHEN file_size_in_bytes < 100663296 THEN 1 ELSE 0 END) AS below_96mib,
       count(*) AS data_files
FROM local.compaction_bench.clean.files
WHERE content = 0;

-- For a partitioned table, find partitions/specs with many small files.
SELECT spec_id, partition, count(*) AS data_files,
       round(sum(file_size_in_bytes) / 1048576.0, 2) AS total_mib
FROM local.compaction_bench.clean.files
WHERE content = 0
GROUP BY spec_id, partition
ORDER BY data_files DESC;

-- Logical validation; SUM(record_count) above is not a substitute with deletes.
SELECT count(*) AS visible_rows, sum(amount) AS amount_sum,
       sum(cast(xxhash64(id, customer_id, event_day, amount, status, payload)
                AS decimal(38,0))) AS hash1,
       sum(cast(xxhash64('second-seed', id, customer_id, event_day, amount, status, payload)
                AS decimal(38,0))) AS hash2
FROM local.compaction_bench.clean;

-- Snapshot operations and summary counts.
SELECT committed_at, snapshot_id, parent_id, operation,
       summary['added-data-files'] AS added_files,
       summary['deleted-data-files'] AS removed_files,
       summary['total-data-files'] AS total_files
FROM local.compaction_bench.clean.snapshots
ORDER BY committed_at DESC;

-- Current manifest sizes and table references.
SELECT count(*) AS manifests, sum(length) AS manifest_bytes
FROM local.compaction_bench.clean.manifests;
SELECT name, type, snapshot_id FROM local.compaction_bench.clean.refs;
```

Run the same file/row checks against `deletes`. Metadata-table scans use their own execution paths; these diagnostic query timings are not the compaction timings. File bounds in metadata are keyed by Iceberg field IDs and often encoded as binary values; decode them with Iceberg's schema/type-aware utilities before comparing actual values.

For future-read measurement, use stable predicates on `customer_id`, a date range and a full-scan aggregation. Preserve identical logical data and query results, cache policy, runtime settings and selected snapshot. The clean and delete fixtures have different visible rows, so they are not interchangeable baselines for a read-latency comparison.

## 8. Other maintenance SQL shapes

These are learning examples, **not measured native Comet results**. Apply them only to the intended lab table. Exact accepted arguments are version-dependent; the [Iceberg 1.8.1 procedure reference](https://iceberg.apache.org/docs/1.8.1/spark-procedures/) is the companion reference.

```sql
-- Consolidate position-delete files, not the ordinary data payload.
CALL local.system.rewrite_position_delete_files(
  table => 'compaction_bench.deletes',
  options => map('rewrite-all', 'true')
);

-- Reorganize manifest metadata; leaves data-file rows/layout unchanged.
CALL local.system.rewrite_manifests(table => 'compaction_bench.clean');

-- Inspect orphan candidates first; this example does not delete them.
CALL local.system.remove_orphan_files(
  table => 'compaction_bench.clean',
  dry_run => true
);
```

Snapshot expiration is intentionally shown as a template because its cutoff and retained count are table-policy decisions:

```sql
-- Replace placeholders only after choosing retention for readers, recovery and references.
CALL local.system.expire_snapshots(
  table => 'compaction_bench.clean',
  older_than => TIMESTAMP '<approved retention cutoff>',
  retain_last => <minimum snapshots to retain>
);
```

Do not use that template during the paired benchmark: the seed snapshot is needed for restoring identical inputs. The original harness expired only its own newly created trial snapshots after saving evidence and rolling back in its isolated warehouse.

## 9. Read the internal plans in Spark History

The saved operator trees, simplified for teaching, read from left to right as data flow:

```text
JVM bin-pack: BatchScan -> ColumnarToRow -> AppendData
Comet bin-pack: CometIcebergNativeScan -> CometIcebergWrite -> IcebergCommit
JVM sort: BatchScan -> ColumnarToRow -> Exchange(range, 9) -> Sort -> AppendData
Comet sort: CometIcebergNativeScan -> CometExchange(range, 9) -> CometSort -> CometIcebergWrite -> IcebergCommit
```

`AppendData` and `IcebergCommit` in the internal group plan hand results to the rewrite coordinator. The **outer action** then performs the replacement snapshot commit. This explains why the group SQL execution duration and full procedure duration differ.

| Case | Saved JVM plan | Saved Comet plan |
| --- | --- | --- |
| Bin-pack | [round 4, SQL 12](assets/compaction/jvm-binpack.plan.txt) | [round 1, SQL 12](assets/compaction/comet-binpack.plan.txt) |
| With deletes | [round 2, SQL 30](assets/compaction/jvm-binpack_deletes.plan.txt) | [round 4, SQL 30](assets/compaction/comet-binpack_deletes.plan.txt) |
| Sort | [round 5, SQL 48](assets/compaction/jvm-sort.plan.txt) | [round 4, SQL 48](assets/compaction/comet-sort.plan.txt) |

These are each engine's median trial, not necessarily a matched round. The evidence JSON retains all timing samples and the original execution/app IDs.

For the original local event logs, open [JVM sort in History](http://localhost:18081/history/local-1791383099643/SQL/execution/?id=48) and [Comet sort in History](http://localhost:18081/history/local-1791382996923/SQL/execution/?id=48). Local links require the matching History Server and event logs; the saved plan files above remain readable without it.

### Which UI numbers to discuss

| Evidence | What it answers | Interpretation limit |
| --- | --- | --- |
| Internal physical plan | Did the scan/sort/exchange/writer actually become native? | Outer CALL alone is insufficient |
| Jobs/stages and task count | Was there shuffle? How much parallelism and skew? | Group count, stage count, task count and output count differ |
| Shuffle read/write and spill | What redistribution/sort pressure occurred? | Encodings and metric coverage can differ between engines |
| Task duration distribution | Was the tail dominated by a few tasks? | Scheduler/GC/I/O/CPU are mixed in elapsed time |
| Driver planning/commit and full CALL timer | How much work remained outside the native group? | Operator timers overlap and do not sum cleanly to wall time |
| Parquet footer writer identity | Was file production native? | Doesn't establish correctness or speed by itself |
| Visible-row checks and file metadata | Was logical content preserved and layout improved? | Does not measure future query speed |

Spark task-thread CPU counters can miss native worker-thread CPU. Do not derive CPU-cost savings from that counter alone. Neither native scan elapsed-compute nor individual write timers should be presented as a complete additive decomposition of the procedure.

## 10. An engaging five-minute demo

```text
Show table problem -> run baseline -> inspect its internal plan -> restore identical input -> run Comet -> compare evidence
```

1. **Problem:** show 256 files totaling only 1.18 GiB. Explain that query planning and file-open work can become disproportionate. State that this is a controlled local fixture.
2. **Baseline:** run one clean bin-pack call in a JVM application. In SQL History show the staged scan, row boundary and JVM writer. Record full CALL time and output file count.
3. **Comet:** restore the exact original seed through the isolated benchmark harness, then run in the separate Comet application. Show `CometIcebergNativeScan` and `CometIcebergWrite`, plus the file-footer identity.
4. **Same job:** both outputs have 10 data files and matching logical checks. Comet reduces execution time; it does not get credit for a different compaction policy or reduced input.
5. **More work:** show the saved sort comparison. The exchange and sort are visible in both arms; Comet substitutes native operators. Finish with the medians across all five rounds, not a lucky live run.

For the delete case, ask: “There are 16M physical records but only 15.2M visible rows. What should compaction write?” The answer is the same 15.2M survivors. Then show why the current delete-file count can remain 256 without invalidating that result.

The presentation's slides 12 and 13 bundle full SQL, both plans, dataset/machine/method and evidence download. Use the notebook when the audience asks how the algorithms or commit boundary work.

## 11. Choosing what to optimize next

The expensive part depends on the observed bottleneck:

```text
Many opens / short tasks -> fix ingestion file sizing or bin-pack
High delete read/reconciliation cost -> rewrite affected data or compact position deletes
Poor pruning -> choose and validate a useful sort/Z-order layout
Slow planning -> inspect manifests and snapshot metadata
Growing retained storage -> inspect snapshot/reference policy and orphan candidates
```

A lower-cost rewrite makes more frequent maintenance feasible, but rewriting too often can waste resources and conflict with active mutations. Prefer measured thresholds and cold/settled partitions when appropriate. Scheduler policy is separate from engine acceleration.

A simple planning estimate is:

```text
maintenance benefit ~= future query count * average query saving - rewrite cost
```

Use consistent units (seconds of resource use or money). This is an engineering estimate, not measured ROI. Include recurring rewrite cost, object requests, shuffle/spill, temporary disk headroom and conflict/retry rates. A wall-time ratio alone is not a cloud-cost ratio.

### Next benchmark dimensions

| Experiment | Why it matters | What remains unmeasured here |
| --- | --- | --- |
| JVM / native child + JVM writer / full native | Separates read/operator gains from native file-production gains | Current result has only two arms |
| Larger data and real partition skew | Exposes memory, parallelism and stragglers | Current fixture is 1.18 GiB and unpartitioned |
| Remote object storage and controlled cache | Separates request/latency effects from local compute | Current result is local and warm-cache |
| Equality deletes / mixed delete density | Exercises a different reconciliation path | Runtime result covers position deletes only |
| Multicolumn Z-order | Tests conversion and multidimensional pruning | Only single-column native test coverage inspected |
| Fixed reads before/after each layout | Quantifies future table-health benefit | No post-compaction read speedup recorded |
| Concurrent ingestion + maintenance | Measures conflicts and backlog | Current paired benchmark is isolated |

## 12. Presenter answers to common questions

**Does Comet fully write Iceberg?** It can produce eligible Parquet data files natively, including encoding and compression. Iceberg Java still rebuilds compatible metadata/commit messages and owns final table publication. This is not universal support for every Iceberg format, write mode, property or maintenance action.

**Why didn't Comet reduce the file count further?** It executes Iceberg's chosen policy and Spark partitions. The benchmark produces the same output counts in both arms; speed is the comparison.

**Are 10 files always healthier than 256?** For this small-file fixture the consolidation target is intentional. For another table, partition boundaries, read selectivity and desired concurrency can change the optimum. Validate actual read behavior.

**Is sorting better than bin-pack?** It buys locality at additional shuffle/sort cost. It is useful only if future predicates and pruning benefit enough. The measured sorting job is slower in absolute time than bin-pack on both engines.

**Does 256 -> 10 mean 25.6x less storage?** No. It means 25.6x fewer current data files. Most live rows remain; compressed bytes need not shrink materially. Retained older snapshots can still occupy storage.

**Does applying deletes remove all delete files?** No. Row visibility, current delete references and retained physical delete objects are separate concerns. The captured delete case preserves visible rows and leaves 256 position-delete files referenced.

**Can EXPLAIN CALL prove acceleration?** Usually the outer procedure plan is not enough. Inspect the internal per-group Spark SQL execution and captured plan. Plan-only inspection is useful for eligible query plans but does not replace executed-plan and file-output evidence for this maintenance demo.

**What exactly improved in this experiment?** End-to-end rewrite wall time for the same seed file layout, with matching logical checks and verified native file output. The result does not isolate scan versus writer gains, establish production cost savings or measure subsequent reads.
