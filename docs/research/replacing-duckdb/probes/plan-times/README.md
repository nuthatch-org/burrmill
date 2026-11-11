# Planning time per rule: the probe, as run

The timers behind `../../where-planning-goes.md` (2026-11-11), kept as patches because the numbers
are only as good as the way they were taken. They are a probe, not a feature: a global mutex and a
string per sample, read from the environment, in a library.

- `plan-times.patch` applies to `9ffacd0`. `df/plantimes.rs` holds the samples.
  `BURRMILL_PLAN_TIMES=<prefix>` rewrites `<prefix>.<pid>` after every statement with the time in
  each analyzer, optimizer and physical rule summed over the process, and again for the dearest
  statement alone. Analyzer and optimizer rules are timed between DataFusion's observer calls, so
  the first optimizer rule of a pass carries the invariant check before it; physical rules are
  wrapped. Lines marked `inside` are spans within a rule, already counted by it, and the three
  counts (plan nodes, shared definitions, relations as `depth.rs` counts them) are the largest
  seen. The spans inside the address and checked rules cost about as much as what they measure
  on a 4,000-node plan; take the split from them and the totals from a run without them.
- The same patch adds three commands to `burrmill-bench`: `plan-times <nest> [runs]` plans each
  view without running it, `join-chain <n>...` answers a statement over `n` one-row relations as
  a star, a chain and a comma list, and `relations <views-dir | sql-file>...` counts relations per
  statement.
- `plan-hashes.patch` goes on top. `BURRMILL_PLAN_DUMP=<prefix>` appends one line per planned
  statement to `<prefix>.<pid>`: a hash of the plan as parsed, of the optimized plan with every
  node's schema, and of the physical plan's operators and schemas. Two builds plan alike when the
  lines agree. Some tests plan statements that differ from run to run (temporary names, the
  clock), so compare against two runs of one build and set aside what differs between those.

For nuthatch's suites, point its `burrmill` dependency at the patched tree with a `[patch]`
section and move the lock to the tree's DataFusion versions with `cargo update -p`.
