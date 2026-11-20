# Where a small statement's time goes, after #371

2026-11-20, for #348. The dialect corpus's 321 statements under 1,000 rows, fresh text each run
(`FRESH=1 burrmill-bench dialect-latency`), burrmill `8963974` built with frame pointers, perf at
1,999 Hz on the ThinkPad, cores 16-23. Shares are of the samples on the `burrmill-statement`
thread, 25,163 of them. The box carried another user's builds (load 13), so read shares, not times.

Fresh text is 1.87x DuckDB's p50 by geomean after #371, from 2.18x; with plans reused, 279 of 321
are at or under DuckDB's p50. What is left on fresh text:

| stage | share | notes |
|---|---:|---|
| analyzer | 23.0% | `Coercion` 6.5% (3.5% of it rebuilding nodes), `DuckComparisons` 1.9%, `Addresses` 1.8%, `CheckedArithmetic` 1.7%, `BoundValues` 1.7%, `FastTextCasts` 1.5%, `DuckSemantics` 1.4% |
| `sql_to_rel` and parsing | 15.5% | mostly DataFusion's `SqlToRel`: `sql_expr_to_logical_expr`, `create_relation`, projections built through `LogicalPlanBuilder` |
| optimizer | 14.5% | `simplify_expressions`, `optimize_projections`, `common_sub_expression_eliminate` the dearest |
| physical rules | about 7% | `EnsureRequirements` 3.3% after #371 left out its distribution pass, `FilterPushdown`, `JoinSelection` |
| initial physical plan | 5.1% | `SegmentTable::scan` about 1.6% of it |
| execution | about 9% | |

Across those stages:

- **Rebuilding projections, 12.3%.** `Projection::try_new`, `projection_schema` and `recompute_schema`
  derive every field's name again by formatting its expression. `fmt::write` alone is 8.7% of the
  thread, called from `Expr::qualified_name`, `schema_name_from_exprs_inner` and `projection_schema`.
  This is DataFusion's, and each rule that changes a node pays it.
- **The allocator, 9.1%.**
- **Equivalence properties, 4.9%**, in the physical stage.

What was looked at and left:

- `BoundValues` changes plans for real: functions are wrapped in `Bounded` when the session registers
  them, and the rule wraps only those planning makes itself (DataFusion's expression planners build
  functions from their own copies, and `||` becomes a concatenation).
- Keeping physical plans for repeated text: a physical plan copies the live tip's rows when it is
  built, so a kept one would answer from a stale tip.
- A substring check ahead of the served-surface refusal: it guards the untrusted-SQL surface, and #371
  instead runs it on the engine's own parse.

The rest is not one fix. The two large blocks are DataFusion's naming of expressions on every schema
derivation and `SqlToRel` itself; either wants an upstream change, or a planning path of Burrmill's own
for statements simple enough not to need DataFusion's.
