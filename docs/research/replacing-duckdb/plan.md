# Plan: replacing DuckDB in nuthatch with Burrmill

Drafted 2026-09-16 from investigations 01-05. It is a plan, not a commitment. Nothing in it touches
nuthatch until Chief says so, and the gates are there to be failed honestly.

## The answer in one paragraph

**Yes, this can be done.** Burrmill hosts DataFusion as its planner and general executor, and
supplies what DataFusion lacks or gets wrong for nuthatch:

- a nest table provider;
- a native hot tip;
- checked arithmetic up to uint256;
- a positive allowlist;
- nuthatch's result encoding;
- the owned fold operators, substituted into DataFusion's plans.

**Speed is not the obstacle.** Correctly configured, DataFusion is 0.71x DuckDB on the real authored
views, and 0.55x on the worst six (04).

**Correctness is achievable, as a policy Burrmill owns** (03).

**The dialect gap is small, and it sits in our own Lodestar and qos views** (01, 04).

**The price is build footprint.** A DataFusion-backed binary is larger than a bundled-DuckDB one,
and that was one of only two wins RFC-0042 found (05). Whether that price is paid in full depends on
one measurement this plan puts first.

## What "better than DuckDB for nuthatch" means here

What each property rests on, and which investigation shows it:

| Property | DuckDB today | Burrmill on DataFusion | Evidence |
|---|---|---|---|
| Exact sums to uint256, refusal on overflow | wraps a parallel `SUM(HUGEINT)` in 1.5.x (duckdb#24081); `DECIMAL` stops at 38 digits, hence `_dec`/`_overflow`; reads Decimal256 Parquet as DOUBLE | 320-bit accumulator, checked scalars, refuses what it cannot represent, order-independent | 03 |
| No filesystem reach from SQL | a directory allowlist added on top; two escape fixes shipped | nothing in the grammar reaches a file; DDL/DML/COPY refused by `SQLOptions` | 02 |
| No native crash on the query path | #1152 assertion, #1165 segfaults | pure Rust; a panic is confined to the query | 01 |
| No libstdc++, older glibc | GLIBCXX 3.4.29, glibc 2.38 on Debian 13 | glibc 2.34, no C++ | 05 |
| Speed on nest-shaped data | baseline | 0.71x overall, 0.55x on the worst six; owned folds 0.5-0.9x | 04, ROADMAP 4.2 |
| Hot tip | copied into a temp table per query | read in place, COR-1 tested | ROADMAP stage 3 |
| Build footprint | 115 MB per test binary, 40 MB release (at `-g0`) | **worse**: 294-313 MB and 119-124 MB via the umbrella crate; about even via component crates (unproven) | 05 |
| Concurrency above 16 clients | 160 qps | 135 qps (owned fold path; DataFusion path unmeasured) | ROADMAP 5.x |
| Cancellation | interrupt handle | scans and aggregates yield; **joins do not** (#19358) | 02 |

## The design

```
nuthatch ──► burrmill::Engine (concrete, non-generic API, RecordBatch/JSON out)
               ├─ parser/linter: sqlparser (DuckDbDialect) + nest rewrites
               ├─ SessionContext, locked:
               │    catalogue   = NestCatalog (explicit segment lists, sizes known,
               │                  footers cached for good, _dec columns, hot tip)
               │    functions   = allowlist, no table functions, no file:// store
               │    SQLOptions  = no DDL, DML or statements
               │    analyzer    = CheckedArithmetic (whitelist: refuse what it does not know)
               │    physical    = FoldSubstitution (owned signed-fold operator)
               │    runtime     = dedicated CPU pool, per-query memory pool,
               │                  shared CacheManager
               └─ encoder: nuthatch's JSON (digit strings, numbers, error text)
```

The parser role (`json_serialize_sql` at 7 sites in nuthatch) moves to a sqlparser AST walk inside
Burrmill, exposed as the same answers nuthatch asks today:

- which tables a statement reaches;
- whether it calls a table function;
- its canonical form.

It must be at least as strict as today. Today it deliberately fails open when DuckDB cannot produce
a parse, with the older denylist still in front (nuthatch `reject_unknown_table_refs`).

## The footprint fork, decided by measurement first

Investigation 05 leaves three routes.

- **U, the umbrella `datafusion` crate.**
  - For: quickest to build on.
  - Against: in the `debug = 0` profile, test binaries are 2.6x DuckDB's and the release binary 3x.
  - In nuthatch's actual shape (about six test binaries since `autotests = false`, not 81), a rough
    estimate is +0.7 GB of dev target (about +25%) and a release binary growing by tens of MB.
    **Estimates, not measurements.**
- **C, component crates plus a Burrmill-owned physical planner** for the plan families the
  workload actually has.
  - Measured lower bound: 43 MB stripped, 140 MB per test binary. That is about even with DuckDB.
  - Not yet counted: the optimizer crates, and the planner itself (about 3k lines if
    `DefaultPhysicalPlanner` is ported, fewer if only the families in use are planned).
- **O, own everything.** 3-4x smaller than both, but the 22 plan families plus DuckDB's syntax make
  it a general engine. Not a plan; it is where the coverage ratchet points in the long run.

**Step 0 of the plan settles U against C with a spike**, before the architecture sets around U by
default.

## Phases and gates

Each phase ends at a gate. If a gate fails, work stops and a keep-amendment records why, as
RFC-0044 §12 says.

### Phase 0: footprint spike (days)

- Build variant C properly: components plus optimizer crates plus a minimal physical planner
  covering the 11 statements that already run (04).
- Measure it with burrmill#1's four figures, in nuthatch's real profile: `line-tables-only`, and a
  nuthatch-shaped test layout of about six binaries.
- **Gate:** C within burrmill#1's limits → C is the shipping route. Otherwise → U, and the footprint
  regression is stated plainly, with numbers, in an RFC-0042 amendment Chief signs. The regression
  is not discovered later.

**Measured 2026-09-17** (`06-footprint-spike.md`). C works (same 90-party answer as DuckDB).
C fails burrmill#1: querying test binary 446 MB against 162, release 105 against 41, target
4.14 GB against 3.27, incremental 1.9 s against 1.1. C still beats U on every size axis
(U is 493 MB / 127 MB). The "otherwise U" fork is the worse failure. If the swap proceeds it
proceeds on C, as a trade, after an RFC-0042 amendment with these numbers. Phase 1 waits on that.

### Phase 1: the engine inside Burrmill (weeks)

1. **`NestCatalog`**: an explicit file list, known sizes, a footer cache sized to the working set,
   statistics off, by-name schema union, declared-but-unsealed tables served empty, and the `_dec`
   columns. Correct for nuthatch's layout (the seal-layout canary already exists).
2. **Lockdown**: `SQLOptions` all false, no table factories, no `file://` store, and a function
   allowlist built from the census (01). A test for each shipped escape (#153, `read_csv`,
   replacement scans) proves it is refused.
3. **`CheckedArithmetic`**: the prototype (03), hardened.
   - A per-type accumulator, to win back about half of the +100-130 MB.
   - A whitelist rule.
   - The literal handled by `parse_float_as_decimal`, or refused at the surface.
   - `TRY_CAST` to decimal for amounts refused, or replaced by the exact text sum.
4. **Result encoder**: nuthatch's JSON exactly. Digit strings for 128-bit and scale-0 decimals, the
   `Debug` fallbacks, and the error phrases `sql_errors.rs` matches on, mapped from DataFusion's own.
5. **Dialect layer**: the three rewrites (04), `UBIGINT` → `BIGINT UNSIGNED`, and
   `information_schema` shaped for the REPL. Where the semantics differ silently:
   - `/` yields DOUBLE in DuckDB;
   - quoted identifiers are case-insensitive;
   - `to_timestamp` returns TIMESTAMPTZ.

   Burrmill either reproduces DuckDB, or refuses and tells the author.
6. **`FoldSubstitution`**: the owned signed fold swapped in by a physical optimizer rule, so the 8
   fold sub-plans run on owned code inside DataFusion-planned statements.

**Gate 1:**

- all 22 graph-allocations statements, and every other nest's views, reach parity once the views
  in item 7 are rewritten;
- time-weighted ≤1.0x DuckDB, per statement ≤1.5x;
- the generated overflow corpus refuses where it should;
- peak RSS at 1M groups, 8 threads, ≤256 MB;
- the phase 0 footprint figures hold.

### Phase 1b: our own views (days, parallel)

7. Rewrite the DuckDB-only constructs in the Lodestar and qos views into portable SQL: 8 ASOF joins,
   7 list comprehensions, 8 `list_reduce`, 3 `TRY()`, 28 `arg_min`/`arg_max`, 22 JSON calls,
   6 hex decodes, and the `port_queue` alias.
   - These are nuthatch files, so this step waits for Chief's go-ahead to touch nuthatch.
   - The rewritten views must still reach parity **on DuckDB**, which makes the step safe to land
     before any engine changes.

### Phase 2: nuthatch integration, DuckDB still in charge (weeks)

8. **An engine trait** behind `/sql`, `/q/{name}`, `/explain`, the CLI, MCP, the RFC-0041 seeds and
   the parser call sites, with DuckDB as the only implementation. A refactor that changes no answer.
9. **Shadow mode, behind a feature flag.** Burrmill answers alongside DuckDB, differences are logged
   with the statement and both answers, and DuckDB's answer is served.
   - It carries two engines and two Arrows (05), so the flag stays off in release builds, and the
     period stays short.
   - Expected differences to classify rather than hide: the `cold_velocity` DOUBLE (01), `/`
     semantics, and DuckDB's wraps on 1.5.x.

**Gate 2:**

- a release cycle of real nest traffic with **zero unexplained differences**;
- p99 within the `/sql` budget (30 s, 2 permits);
- memory within nuthatch's RFC-0047 envelope;
- the parser replacement at least as strict as `json_serialize_sql` on the security corpus.

### Phase 3: cutover and removal

10. Burrmill becomes the default engine, and DuckDB becomes a dev-dependency oracle (RFC-0044 slice
    7; RFC-0042 §3a holds: no user query on DuckDB in a shipped binary).
11. DuckDB is removed after one clean release (slice 8). The generated corpus and the `.slt` files
    stay, with the reference oracle, as the regression suite.

## Risks that could stop it

- **Footprint.** If C is not viable and the U regression is unacceptable, the swap is a trade, not
  a win, and it should be decided as one.
- **Joins do not yield to cancellation** (#19358, fix abandoned). Statements with joins cannot
  promise the one-morsel cancellation bound. Mitigations are a per-query timeout that drops the
  stream (acceptable at nuthatch's 30 s budget), or owning the join yield point.
- **Memory under DataFusion's pools** (#20714, #24994), at the 256 MB gate. Unmeasured for the
  DataFusion path.
- **DataFusion churn.** Dozens of breaking changes per major (43 in 55.0), so every quarter carries
  a pin-and-bump cost. The concrete-API rule (burrmill#1) keeps it inside Burrmill.
- **Concurrency above 16 clients.** Measured only for the owned fold path. The DataFusion path
  needs its own `serve` sweep before gate 2.
- **Silent semantic differences** that nothing errors on. Shadow mode exists for exactly these, and
  it must run on real traffic, not fixtures.

## Independent of the swap, worth doing now

- **nuthatch `cold_velocity`**: the cold seed is always empty
  (`docs/upstream/nuthatch-cold-velocity-seed-empty.md`). Chief's call.
- **duckdb#24081**: request a backport to 1.5 (`docs/upstream/duckdb-hugeint-parallel-wrap.md`).
  Chief's call.
- **DuckDB `BIGNUM`** already sums uint256 exactly (03), and could replace `_overflow` while DuckDB
  remains.
- **Burrmill:**
  - trim Arrow's default features (05);
  - set `parquet_metadata_cache` in `views.rs` and `serve.rs` and re-run 4.2 (4.2c);
  - re-run the synthetic sweep under the remedied DataFusion configuration, which would retire the
    README's 3.6x;
  - run a DataFusion-path `serve` sweep.
