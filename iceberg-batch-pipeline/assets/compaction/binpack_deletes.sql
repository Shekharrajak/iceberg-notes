-- Query shape: Scan + position-delete application + file write; no exchange or sort.
-- Purpose: Materialize the surviving rows while consolidating small files.
-- Timed end to end, including the Iceberg snapshot commit.

CALL local.system.rewrite_data_files(
  table => 'compaction_bench.deletes',
  strategy => 'binpack',
  options => map('rewrite-all','true',
    'target-file-size-bytes','134217728',
    'max-concurrent-file-group-rewrites','1',
    'partial-progress.enabled','false'));
