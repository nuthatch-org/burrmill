# Where planning time goes, rule by rule

2026-11-11. Three issues filed from a cold read of the planner (#154, #155, #157) said which
passes ought to be slow, and none was measured. This is the measurement: every analyzer,
optimizer and physical rule timed on the statements RFC-0045 §3.3 is about. Thinkpad, 32 cores,
release, burrmill `9ffacd0` with the timers in `probes/plan-times/`. The box carried other jobs
all day (load 60 to 120 on 32 cores), so a millisecond here is three or four of a quiet day's.
Read the shares, and a before against an after only where the two ran back to back, as the pair
below did. Transcript: `docs/bench/plan-rules-thinkpad-2026-11-11.txt`.

## Workloads

- **The network contract**: nuthatch's `tests/network_contract.rs` (`c0efbf55`, `--features
  graph`) on Burrmill, one thread. 240 statements over the network nest's views, about thirty
  views to a statement, up to 4,768 logical and 3,709 physical nodes.
- **The allocations nest**: `~/1165-corpus/graph-allocations-nest-next`, 1,925 segments, each
  view planned nine times and not run (`burrmill-bench plan-times`). 13 of its 19 views register
  on `main`; two do not parse and four read those two. Plans are small, 238 logical nodes at
  most, and each scan lists up to 1,925 files.

## The whole of it

The contract, 429 s of planning under load, 1.8 s a statement:

| stage | share | the dearest in it |
|---|---:|---|
| parse, views to plans | 8.2% | nuthatch defines the nest's views for every session |
| analyzer | 42.5% | `type_coercion` 10.9% (DataFusion's, run twice), `addresses` 8.0%, `share_repeats` 3.8%, `fast_text_casts` 3.6%, `checked_arithmetic` 3.1%, `duck_comparisons` 2.5%, `duck_semantics` 2.3%, `move_constants` 2.2% |
| logical optimizer | 36.9% | DataFusion's rules over three passes, `optimize_projections` 4.7% the dearest; `prune_shared` 2.7% |
| physical | 12.4% | `EnsureRequirements` 5.2%, `ProjectionPushdown` 2.1%, the initial plan 1.8%, `join_selection` 1.3% |

Burrmill's own analyzer rules are 31% of planning between them, and DataFusion's optimizer is
the largest single block. The allocations nest has the same shape at a thirtieth of the size:
analyzer 48%, optimizer 30%, physical 18%, `addresses` 7.9%, `checked_arithmetic` 3.6%.

## #154: the physical wrappers

| rule | contract, 240 statements | its dearest statement |
|---|---:|---:|
| `compact_views` | 930 ms | 3.4 ms |
| `held_input` | 742 ms | 1.2 ms |
| `cancellable` | 369 ms | 4.0 ms |
| `text_address_filters` | 365 ms | 7.5 ms |
| `computed_key_filters` | 124 ms | 2.1 ms |
| `topk_filters` | 44 ms | 0.7 ms |
| `range_join` | 38 ms | 0.6 ms |
| together | 2,612 ms of 429,234 | 19.5 ms of 5,258 |

0.61% of planning and 4.9% of the physical stage; 0.6% on the allocations nest. Applying them in
one pass would save at most half a percent. No change. The `equivalence::properties` time is
`EnsureRequirements`, as §3.3 says.

## #155: `bytes_read`

15,682 calls and 216 ms on the contract, 0.05%; 103 calls and 0.7 ms on its dearest statement;
756 calls and 2.4 ms on the allocations nest, where a scan has the most files, 0.03%.
`small_inputs` as a whole is 0.27% and `build_on_smaller` 0.07%. No change.

## #157: the copies and the walks

Measured, then changed in order of what each could give back. The contract, main's rules and
then the change, run back to back; the rules nobody touched agree to within one percent
(`share_repeats` 16.4 s and 16.6 s, the optimizer 158.3 s and 157.7 s):

| item | before | what was done | after |
|---|---:|---|---:|
| `type_coercion` (both runs; the second is over shared definitions), `duck_doubles` and the five correlation rules, which run over them too | 57.7 s | the definitions node is dropped before a rule runs, so the rule's walk moves the plans it used to copy node by node; and after a rule retypes definitions, each later plan is refreshed once for all of them, rebuilding only the nodes over a reference (6.1 s in 1,348 walks became 0.7 s in 643) | 44.8 s |
| `checked_arithmetic` | 13.3 s | an aggregate's input is walked for what it taints once, not once per aggregate expression; one input's schema is borrowed where it was copied field by field; a name is formatted only where a rename is possible | 10.1 s |
| `addresses` | 34.1 s | a node's schema is derived again only where its expressions or an input's schema changed; schema and names as above | 31.1 s |
| `prune_shared` | 11.4 s | nothing: 9.3 s of it is the `OptimizeProjections` run that tells it what each reader takes, which it cannot learn another way | 11.3 s |
| the analyzer | 182.3 s | | 163.1 s |
| all planning | 429.2 s | | 410.0 s |

Planning is 4.5% shorter and the analyzer 10.5%. On the allocations nest the total does not move
(6,055 ms and 6,070 ms): `checked_arithmetic` falls 13% there and `addresses` 1%, and nothing is
shared often enough for the rest to matter.

`addresses` moves less than its size suggests. `recompute_schema` is 42% of the rule, and in a
statement over thirty views nearly every node sits above a scan whose addresses just became
fixed binary, so nearly every schema does change and has to be derived again.

The plans are the same. Each statement's optimized logical plan was printed with every node's
schema, shared-subquery ids renumbered by first appearance, and compared between main's rules
and the change: 130 statements and 128,686 plan lines of the contract with none differing, 120
statements of `serve::tests` with three differing only in a `now()` they hold as a literal, and
793 statements of Burrmill's own suite by hash. Dialect parity is 370 of 370 both ways, and the
release gate's 117 statements over six production nest copies answer as on `main`.

## The relation bound

`MAX_RELATIONS` was 256. One-row relations, release, least of three, the whole statement:

| relations | star of `JOIN ... ON` | chain | comma list |
|---:|---:|---:|---:|
| 64 | 0.21 s | 0.22 s | 0.09 s |
| 96 | 0.52 | 0.67 | 0.14 |
| 128 | 1.19 | 1.58 | 0.30 |
| 192 | 4.05 | 5.27 | 0.78 |
| 256 | 9.85 | 14.36 | 1.42 |

At 128 the chain plans for 1.7 s, 1.3 of it in `eliminate_cross_join`; at 256 for 15.1 s, 14.1
of it there, and a cancel is only seen between rules. The largest real statement reads 65
relations as the depth walk counts them (the `lodestar_indexers` view; 63 in the network nest
and at run time through the contract and `serve::tests`). The bound is 128 since #271.

## What was not touched and shows up

`share_repeats` (3.8%), `fast_text_casts` (3.6%), `duck_comparisons` (2.5%), `duck_semantics`
(2.3%) and `move_constants` (2.2%) were in no issue and are each as large as anything fixed
here. `addresses` is still the dearest rule of ours. Shared-subquery ids differ from one run to
the next of the same build, which no answer depends on and every plan comparison has to work
around.
