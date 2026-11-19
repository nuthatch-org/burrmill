# Vendored crates

## burrmill-parquet-source 55.2.0

DataFusion's `datafusion-datasource-parquet` 55.2.0 from crates.io, unchanged except for the
per-file metrics in `src/opener/mod.rs` and one field in `src/source.rs`. Published under its own
name, and `burrmill` takes it as `datafusion-datasource-parquet` by renaming the dependency, so
nuthatch and crates.io consumers get the change without a `[patch]` of their own. Nothing else in
DataFusion depends on the crate, so no second copy of its types enters burrmill's graph; the bench's
stock arm, which pulls the umbrella `datafusion`, gets the upstream crate, as a control should.
Package name, description, repository and lib name changed, upstream's benches dropped. A
DataFusion bump must re-apply the change to the new release and publish it before `burrmill`, or
drop this copy if upstream has an equivalent.

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
