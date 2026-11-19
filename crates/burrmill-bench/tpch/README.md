# TPC-H queries

The 22 queries as apache/datafusion 55.2.0 carries them in `benchmarks/queries/` (Apache-2.0),
derived from the TPC-H specification. Q15 is the one change: its `CREATE VIEW` / `DROP VIEW` pair
is written as a `WITH`, because Burrmill serves queries only, and all three engines run that text.
`burrmill-bench tpch` runs them (docs/bench/tpch.md).
