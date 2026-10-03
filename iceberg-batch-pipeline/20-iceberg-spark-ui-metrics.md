# Iceberg metrics in Spark UI: planning, execution and commit evidence

Source inspected October 3, 2026; observations below come from the saved October 2 demo, not a new benchmark. Read [chapter 19](19-reading-spark-ui.md) for timings, query parameters, app/execution IDs, screenshots and run commands.

## TL;DR

| Question | Best evidence | What our demo establishes |
| --- | --- | --- |
| What work did Iceberg select? | Scan-node planning counters, filters, snapshot and manifest/file metadata | JVM and Comet select the same file counts in the examples below |
| What did the reader actually do? | Runtime rows, splits, bytes, scan time and deletes-applied counters where supported | Reader metric sets and filter boundaries differ |
| Where did Spark move/process data? | SQL exchanges/joins, job DAG, stage task metrics | Local partitioned execution; not multi-machine network performance |
| Did files become committed table state? | Successful commit and snapshot summary, current files metadata | Read-query screenshots do not establish this |
| Did compaction reduce fragmentation? | Procedure result, before/after file layout, checksum and elapsed time | Separate maintenance harness records these; do not infer them from Q1/Q3/Q9/Q18 |
| Did cleanup delete cloud objects faster? | Procedure results plus storage-side telemetry and measured interval | Not established by this local-filesystem demo |

The talk-ready sentence: **Iceberg counters explain selected work; reader counters explain executed work; Spark stages explain coordination; snapshot metadata proves committed table state.**

## Four layers, four measurement boundaries

```text
Planning: snapshot -> manifests -> partition/file pruning -> data tasks + applicable deletes
Execution: file splits -> reader + deletes + predicates -> rows/batches -> relational operators
Distribution: exchange writer -> shuffle blocks -> fetch/decode -> receiving tasks
Write: task files -> commit messages -> driver validation -> snapshot publication
```

DataSourceV2 provides the interface for custom scan metrics; it does not automatically manufacture every useful Iceberg metric. Iceberg's `SparkScan.supportedCustomMetrics()` declares supported metrics and `reportDriverMetrics()` converts a `ScanReport` into driver metrics. Reader tasks supply a different set of runtime counters. Comet forwards planning metrics from the original scan and supplies its own native runtime metrics.

Source: [SparkScan.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScan.java#L283), [CometIcebergNativeScanExec.scala](../../../../oss/apache/datafusion-comet/spark/src/main/scala/org/apache/spark/sql/comet/CometIcebergNativeScanExec.scala#L185). The reference Iceberg checkout is `5e7169168d`; the captured runtime jar is Iceberg 1.8.1. Use saved runtime evidence for exact displayed names/values, not assumptions that the reference checkout is identical to the jar.

## Metric dictionary: what the labels actually mean

| Planning metric key | Meaning | Common misreading |
| --- | --- | --- |
| `totalDataManifest`, `totalDeleteManifests` | Manifest counts reported for the scan | All manifests ever created for the table |
| `scannedDataManifests`, `scannedDeleteManifests` | Manifests scanned during planning | Exact number of remote GET requests or cache misses |
| `skippedDataManifests`, `skippedDeleteManifests` | Manifests excluded from the scan | Each skipped manifest equals one skipped data file |
| `resultDataFiles` | Data files selected into file tasks before task splitting | Spark task count, partitions in the table or rows returned |
| `skippedDataFiles` | File entries rejected by planning filters | Every file omitted from the whole table, including all entries inside skipped manifests |
| `totalDataFileSize` | Selected data-file sizes accumulated by planning | Actual projected/compressed bytes read over the network |
| `indexedDeleteFiles` | Delete files added to the delete index | Rows removed by those files |
| `positionalDeleteFiles`, `equalityDeleteFiles` | Delete-index categories | Two runtime CPU-time measurements |
| `resultDeleteFiles` | Delete-file associations accumulated for planned data files | Necessarily a globally distinct file count |
| `totalDeleteFileSize` | Delete content sizes accumulated for those associations | Necessarily physical I/O bytes after caching/reuse |
| `skippedDeleteFiles` | Delete-file planning exclusions | Rows surviving deletion |
| `totalPlanningDuration` | Iceberg-reported scan planning duration | Entire Spark planning time or SQL wall time |

The source distinction matters: `ScanMetricsUtil.indexedDeleteFile()` increments index/type counters; `fileTask()` increments `resultDeleteFiles` by the length of the delete-file array attached to each data file. A shared delete file can therefore contribute to multiple task associations. Similarly, manifest skipping can avoid visiting contained file entries, so `skippedDataFiles / (skippedDataFiles + resultDataFiles)` is not universally a table-wide pruning percentage.

Source: [ScanMetricsUtil.java](../../../../oss/apache/iceberg/core/src/main/java/org/apache/iceberg/metrics/ScanMetricsUtil.java#L33), [ManifestGroup.java](../../../../oss/apache/iceberg/core/src/main/java/org/apache/iceberg/ManifestGroup.java#L340).

<details>
<summary>Can these counters prove partition evolution helped?</summary>

No. Iceberg can plan files written under different partition specs, but selected/skipped counters do not attribute the benefit to spec evolution versus partition predicates, file statistics or other filters. For that claim, inspect spec IDs, file layout and predicate projection, then compare controlled scans. A table with no partition evolution can also show strong file pruning.

Likewise, a skipped file counter does not prove page-index pruning, SIMD, fewer allocations or faster decoding. Those happen at other layers. Metadata reading is still work even when data files are never opened for row decoding.

</details>

## Q1: exact JVM versus Comet scan comparison

These are normal-mode measured iteration 5, not the five-run median. The result is four rows with matching schema/count/digest. The names in the two UIs differ, so compare semantics rather than literal display strings.

| Metric | JVM `BatchScan local.tpch.lineitem` | `CometIcebergNativeScan` |
| --- | --- | --- |
| Result data files | 81 | 81 |
| Skipped data files | 2 | 2 |
| Scanned data manifests | 1 | 1 |
| Skipped data manifests | 0 | 0 |
| Scanned delete manifests | 1 | 1 |
| Indexed / positional delete files | 80 / 80 | 80 / 80 |
| Equality delete files | 0 | 0 |
| Result delete-file associations | 80 | 80 |
| Skipped delete files | 1 | 1 |
| Planning duration | 5 ms | 5 ms |
| Planned data-file bytes | 1,967,029 | 1920.9 KiB, UI-rounded |
| Planned delete-file bytes | 140,959 | 137.7 KiB, UI-rounded |
| File splits read/processed | 81 | 81 |
| Row deletes applied | 601 | Not exposed |
| Scan output rows | 59,153 | 59,068 |
| Following filter output rows | 59,068 | 59,068 |

[JVM node metrics](assets/spark-ui/normal-q1-jvm-metrics.json) - [Comet node metrics](assets/spark-ui/normal-q1-comet-metrics.json) - [JVM screenshot](assets/spark-ui/normal-q1-jvm-sql.png) - [Comet screenshot](assets/spark-ui/normal-q1-comet-sql.png).

```text
JVM: columnar scan -> 59,153 rows -> downstream filter -> 59,068 rows
Comet: native scan -> 59,068 rows -> downstream filter -> 59,068 rows
```

Observed: the scan boundary reports 85 fewer rows in Comet; both streams agree after filtering. Interpretation: predicate work can occur at different points, so compare equivalent logical boundaries. This is not 85 extra deleted rows and not evidence of incorrect results. The row counts alone cannot isolate whether page selection, decode-time filtering or another reader step supplied that reduction.

The current local Comet source excludes the Java `numDeletes` counter because the native reader does not provide the corresponding deletes-applied count. Reporting zero would be misleading. Absence is a metric-coverage gap, not zero deletion work. Comet's scan time is native elapsed-compute reporting aggregated across tasks, not the SQL wall-clock duration; `bytes_scanned` is also distinct from planned full-file sizes.

## Q3 and Q9: selective versus broad scans

Normal-mode measured iteration 5; values below are the same in JVM and Comet for planning counts. Table association is determined from the saved physical plans, not merely the repeated Comet scan-node name.

| Query / table | Result data files | Skipped data files | Result delete-file associations | JVM scan rows | Comet scan rows |
| --- | --- | --- | --- | --- | --- |
| Q3 customer | 1 | 0 | 0 | 1,500 | 337 |
| Q3 orders | 39 | 41 | 32 | 7,309 | 7,212 |
| Q3 lineitem | 45 | 38 | 44 | 32,316 | 31,938 |
| Q9 part | 1 | 0 | 0 | 2,000 | 2,000 |
| Q9 supplier | 1 | 0 | 0 | 100 | 100 |
| Q9 partsupp | 1 | 0 | 0 | 8,000 | 8,000 |
| Q9 orders | 80 | 0 | 67 | 14,850 | 14,850 |
| Q9 lineitem | 83 | 0 | 81 | 59,572 | 59,572 |
| Q9 nation | 1 | 0 | 0 | 25 | 25 |

Evidence: [Q3 JVM](assets/spark-ui/normal-q3-jvm-metrics.json), [Q3 Comet](assets/spark-ui/normal-q3-comet-metrics.json), [Q3 native plan](assets/spark-ui/normal-q3-comet.plan.txt), [Q9 JVM](assets/spark-ui/normal-q9-jvm-metrics.json), [Q9 Comet](assets/spark-ui/normal-q9-comet-metrics.json), [Q9 native plan](assets/spark-ui/normal-q9-comet.plan.txt).

Q3's order/ship-date conditions exclude many files. The customer segment predicate also demonstrates earlier row filtering in the native path even when the one customer file cannot be pruned. Q9's `moccasin` name predicate does not produce data-file skipping in this capture. Zero skipped files is not proof that pushdown is broken: metadata may be unable to reject a file containing both matching and nonmatching rows.

The teaching contrast: **Comet can improve execution even when Iceberg selects the same files.** Chapter 19's paired timings demonstrate an observed whole-query difference; these counters do not apportion it among scans, joins, conversion, aggregation and shuffle.

## Q18: planned work is not the same as completed runtime work

The normal Q18 plan contains two lineitem scan nodes, semi joins and reused exchange work. Both engines return zero final rows. The saved Comet metrics include scan nodes with planning counters but no output-row metric value in the exported node data. Do not fill those absent values with zero or assume every planned branch did equal work.

Use [Q18 JVM metrics](assets/spark-ui/normal-q18-jvm-metrics.json), [Q18 Comet metrics](assets/spark-ui/normal-q18-comet-metrics.json) and the [saved plan](assets/spark-ui/normal-q18-comet.plan.txt) together. Counting all scan boxes is not a distinct table-file inventory; the same table appears more than once, and reuse/empty inputs affect execution. Establish job/stage/task activity before interpreting missing counters. Q18 remains a semantics example until a nonempty representative run is measured.

## Writes: distinguish task output from committed state

```text
Executor writes files -> reports records/bytes + commit messages -> driver validates -> catalog publishes snapshot
```

Iceberg's Java `SparkWrite.TaskCommit.reportOutputMetrics()` reports bytes and records from task files into Spark task output metrics. It does not turn those counters into proof that the driver committed a snapshot. An attempt may write files before abort, retry or commit failure. Native write metric coverage must be inspected separately; do not assume Java writer instrumentation is shared automatically.

| Question | Evidence to inspect |
| --- | --- |
| How many task records/bytes were produced? | Stage/task output metrics; attempt status and retries |
| What new files became visible? | Successful snapshot's summary plus current file metadata |
| Which partitions received files? | Current files metadata partition values/spec IDs; distinguish new files from existing files |
| How many files did this operation add? | Snapshot summary such as `added-data-files`, when present; correlate the correct snapshot/operation |
| Was native writing actually used? | Executed operator plan and relevant task metrics, not just Comet enabled in config |
| Was time spent committing rather than writing? | Commit instrumentation/logs correlated with task completion; SQL duration alone cannot separate them |

Read-only inspection examples; use the actual target table created by the demo:

```sql
SELECT committed_at, snapshot_id, operation, summary
FROM local.demo.target.snapshots
ORDER BY committed_at DESC;

SELECT content, spec_id, partition, count(*) AS files,
       sum(file_size_in_bytes) AS file_bytes
FROM local.demo.target.files
GROUP BY content, spec_id, partition;
```

These current-file totals are not automatically per-write deltas. Match the operation's snapshot, account for concurrent commits and distinguish data files from delete files using `content`. There is no verified generic "Dynamic Partition Alignment" metric in our capture. Runtime scan filtering, write distribution requirements and the set of partitions updated are distinct facts.

Source: [SparkWrite.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java#L640). The demo's `src/iceberg_pipeline_demo.py` records INSERT elapsed time including driver commit; the read comparison runner does not test write throughput.

## Maintenance: use procedure results and before/after state

| Operation | What to inspect | What not to infer |
| --- | --- | --- |
| `rewrite_data_files` | Rewrite job DAG; returned `rewritten_data_files_count` / `added_data_files_count`; file sizes/counts before and after; checksum | Every CALL's command-node plan exposes all internal native operators |
| `expire_snapshots` | Returned deleted data/delete/manifest counts; snapshot history before and after; current table still readable | Removing snapshots means all their files are deleted; retained snapshots can still reference shared files |
| `remove_orphan_files` | Returned orphan locations; safe retention policy, scope and dry-run where supported | A file absent from a recent scan is orphaned or safe to remove |

Apache Iceberg's Spark procedure here is `rewrite_data_files`; vendor `OPTIMIZE` syntax is not a universal synonym. Compaction is read/rewrite work followed by a metadata commit. Snapshot expiry and orphan cleanup do not rewrite table rows, but can perform real physical file deletion; calling them purely metadata-only can hide substantial I/O and risk.

Our separate `04-run-maintenance-demo.sh` uses `iceberg_pipeline_demo.py` to save rewrite procedure results, before/after file statistics and matching checksums. It separately measures snapshot expiry and records snapshot counts. The script's aggressive expiry cutoff is for its disposable demo table, not a production retention recipe. Merely finding the code does not establish a fresh successful run; inspect its generated summary for the particular execution.

The saved Q1/Q3/Q9/Q18 UI evidence contains reads, not these maintenance measurements. No cloud deletion-rate claim follows from local file operations. To report a deletion rate, define successful objects deleted and the measured interval, distinguish attempts/retries, and collect relevant storage/client telemetry. A thread pool doing deletion need not map one object to one Spark task.

Sources: [RewriteDataFilesProcedure.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java#L70), [ExpireSnapshotsProcedure.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/ExpireSnapshotsProcedure.java#L73), [RemoveOrphanFilesProcedure.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RemoveOrphanFilesProcedure.java#L90).

## Presenter walkthrough and troubleshooting

1. Open Q1's matched execution pair from chapter 19. Show 81 selected files, two skipped files and equal planning metrics: same planned input does not imply same execution cost.
2. Show positional deletes and the JVM 601 deletes-applied counter. Explain why Comet does not expose an equivalent counter, rather than saying it did no delete work.
3. Follow rows through scan and filter: compare at an equivalent logical boundary, then verify final schema/count/hash.
4. Open Q3: show file pruning first, then forced-mode exchanges/sorts/joins. Iceberg file pruning and Spark shuffle partitioning solve different problems.
5. Open Q9: no data-file pruning here, but a longer processing chain. Use paired medians, not summed node times, for latency comparison.
6. Treat Q18's empty result as an execution/metric caveat. Switch to the separate write/maintenance evidence only when explaining those operations.

<details>
<summary>Why are Iceberg metrics missing or suspicious in History Server?</summary>

- Verify this is an Iceberg scan of the expected snapshot, not a Parquet-path read or a different execution.
- Distinguish unsupported counters from valid zero values, omitted values and unexecuted branches. A planned operator need not have a completed runtime measurement.
- Include the matching Iceberg/Comet jars when replaying custom metrics. The demo's History Server helper handles this; inspect logs for class-loading failures.
- Use uppercase `/SQL/` paths. SQL REST listings are paginated; do not conclude a later execution is absent from the application because it was absent from the first page.
- Verify the query label/iteration and app ID. SQL IDs alone do not match queries across applications.
- In the local Comet scan implementation, driver metrics are guarded against repeated posting because summed updates could otherwise double-count planning values. Suspicious multiples warrant source/event inspection, not an assumption of twice the I/O.
- Compare units: raw bytes versus rounded KiB, summed task time versus wall time, whole-application executor totals versus a selected SQL execution.

</details>

## Validation, source scope and remaining gaps

Inspected [TestSparkReadMetrics.java](../../../../oss/apache/iceberg/spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkReadMetrics.java#L45): its v1 fixture creates two files and asserts one result data file and one skipped data file, along with manifest/planning counters. This is inspected coverage, not a test run in this documentation task. Newer reference-source metrics do not imply availability in the 1.8.1 runtime.

The user supplied a video link and a prose description of possible UI metrics. Its page/caption-track metadata were retrievable, but the transcript endpoint returned no text. This chapter is a source-and-capture comparison, not a verified video transcript summary or attribution to its speaker.

No engine code changed and no query, write, cleanup or benchmark was rerun for this chapter. Durable captured evidence is linked throughout. Remaining work for a stronger performance claim: representative scale, nonempty Q18, isolated reader/operator profiling, controlled layout comparisons, multi-executor networking and separately captured write/maintenance results.
