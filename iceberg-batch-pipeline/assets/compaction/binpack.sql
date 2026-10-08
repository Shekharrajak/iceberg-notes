-- Query shape: Scan + native/JVM file write; no exchange or sort.
-- Purpose: Consolidate small files without changing the sort order.
-- Timed end to end, including the Iceberg snapshot commit.

CALL local.system.rewrite_data_files(
  table => 'compaction_bench.clean',
  strategy => 'binpack',
  options => map('rewrite-all','true',
    'target-file-size-bytes','134217728',
    'max-concurrent-file-group-rewrites','1',
    'partial-progress.enabled','false'));
