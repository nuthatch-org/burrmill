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

## Per rule, 2026-11-21

The question was whether to fuse the analyzer's 29 rules into fewer walks. Timed per rule over the
same corpus (`probes/rule-times/`, an M5 Pro, fresh text, 3,561 statements planned):

| stage | rules | time | of it, on calls that changed nothing |
|---|---:|---:|---:|
| analyzer | 29 | 630 ms | 388 ms |
| optimizer | 26 | 379 ms | 169 ms |
| physical | 30 | 331 ms | 87 ms |

So the analyzer spends about 109 µs a statement changing nothing, against a gap to DuckDB of about
330 µs. The plans average six nodes, so that is not the walk: an unchanged `addresses` call costs
9.7 µs, some 2 µs a node, which is expression work and schema derivation inside the rule. Fusing
the walks would keep all of it. Not worth doing.

The cost is concentrated. Six rules carry 252 ms of the analyzer's unchanged time, about 71 µs a
statement:

| rule | unchanged time | each |
|---|---:|---:|
| `type_coercion` | 124 ms | 23 µs |
| `addresses` | 35 ms | 10 µs, and never changed a plan here |
| `duck_semantics` | 27 ms | 10 µs |
| `checked_arithmetic` | 23 ms | 9 µs |
| `duck_doubles` | 22 ms | 7 µs |
| `duck_comparisons` | 21 ms | 8 µs |

Each wants what #371 gave `fast_text_casts` and `move_constants`: a read-only look that returns the
plan untouched when there is nothing to rewrite. `type_coercion` is DataFusion's, run inside
`coercion::First`; the rest are Burrmill's. On DataFusion's side `eliminate_join`,
`extract_leaf_expressions` and `rewrite_set_comparison` never changed a plan and cost 57 ms
between them, 16 µs a statement. Together that is some 87 µs of a fresh statement's 703, which is
another cheap win, not the structural part.

## After #376, on the ThinkPad

Frame pointers again, `980b0ea`, cores 16-23, 33,337 samples on the statement thread. Fresh text is
2.03x DuckDB's p50 by geomean on this box.

| stage | share |
|---|---:|
| analyzer | 23.0% |
| `SqlToRel` | 15.7% |
| optimizer | 15.0% |
| parse (`DFParser`, 3.2% of it the tokenizer) | 10.9% |
| physical rules | 9.8% |
| initial physical plan | 5.1% |
| execution | 7.3% |
| the rest, Burrmill's own glue | 12.6% |

The optimizer's work between rules, which the Mac's per-rule timers put at about 65 µs a statement,
is 2.9% of the thread here: `LogicalPlanSignature::new` 1.0%, `rewrite_with_subqueries` 0.6%,
`check_invariants` 0.4%. Most of the Mac figure was the timers' own node counts.

Nothing left is one thing. Parse is spread over sqlparser's recursive descent, with no function above
0.9%. `format!` is 5.3% of the thread, and its largest callers are `projection_schema` naming every
field again (from coercion's `recompute_schema`, from `SqlToRel` building projections, and from
`optimize_projections`) and `ScalarUDFImpl::schema_name`. Coercion's recompute cannot simply be left
out where nothing changed: tried, it changed the answer on 16 corpus statements, because an earlier
rule leaves an aggregate's schema stale and coercion is what repairs it.

What is left wants DataFusion to name an expression once rather than at every schema derivation, or
a planning path of Burrmill's own.

## Naming once, 2026-11-21

Most of that naming was waste. `Expr::to_field` formats a name for every subexpression on its way
down and the caller renames the field anyway, and `get_type` and `nullable` on any function call go
through it, so coercion formats every argument's name to learn its type. `vendor/datafusion-expr`
carries a change that names only the outermost expression (see `vendor/README.md`). On the Mac,
three interleaved pairs of `FRESH=1 burrmill-bench dialect-latency`:

| | geomean vs DuckDB p50 | Burrmill p50 sum | DuckDB p50 sum |
|---|---:|---:|---:|
| upstream | 1.81, 1.79, 1.75 | 219, 215, 193 ms | 127, 126, 116 ms |
| naming once | 1.65, 1.61, 1.60 | 203, 178, 177 ms | 127, 116, 116 ms |

About 11% off a fresh statement, twice `format!`'s 5.3% share; where the rest comes from is not yet
profiled. Dialect parity is unchanged at 370 cases. Upstream main has the same code.
