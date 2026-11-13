# Vendored crates

## datafusion-datasource-parquet 55.2.0

The crates.io release, unchanged except for the per-file metrics in `src/opener/mod.rs` and one
field in `src/source.rs`. Carried under `[patch.crates-io]` in the workspace `Cargo.toml`; a
DataFusion bump must re-apply the change to the new release, or drop this copy if upstream has
an equivalent.

Upstream, `ParquetMorselizer::prepare_open_file` registers a fresh `ParquetFileMetrics`, a
`BaselineMetrics` and a `num_predicate_creation_errors` counter into the scan's one
`ExecutionPlanMetricsSet` for every file it opens. Nothing removes them, and the plan and every
partition's reader hold the set, so a scan held about 4.6 KB per segment until the plan dropped:
89 MB a scan at 15,108 segments.

The change:

- `src/opener/mod.rs`: a `PartitionMetrics` holding those three, made once per partition by
  `PartitionMetrics::new`, and a `partition_metrics` field on `ParquetMorselizer`.
  `prepare_open_file` clones them in place of the three registrations (upstream lines 733-736 and
  806-808), and the test builder sets the field. Each file still gets its own `ParquetFileMetrics`
  struct, sharing the partition's counters.
- `src/source.rs`: `create_morselizer` sets `partition_metrics`.

What `EXPLAIN ANALYZE` loses: per-file rows of the parquet metrics. Counters and timings are
summed by name when it prints, so their totals are the same. The gauges are not:
`predicate_cache_inner_records` and `predicate_cache_records` are set per file, so a shared one
shows only the last file's value, and `scan_efficiency_ratio` adds the partition's running
`bytes_scanned` for every file, so it overstates. These are display only; no budget reads them.
The `filename` label now reads `(every file)`, and the baseline metrics' `end_time` is the
partition's, not the last file's.
