-- Query shape: Scan + range exchange + sort + file write.
-- Purpose: Sort by customer_id, then id; the captured exchange uses 9 partitions.
-- Timed end to end, including the Iceberg snapshot commit.

CALL local.system.rewrite_data_files(
  table => 'compaction_bench.clean',
  strategy => 'sort', sort_order => 'customer_id ASC NULLS FIRST, id ASC NULLS FIRST',
  options => map('rewrite-all','true',
    'target-file-size-bytes','134217728',
    'max-concurrent-file-group-rewrites','1',
    'partial-progress.enabled','false'));
