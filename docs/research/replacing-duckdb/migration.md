# Migration: nuthatch from DuckDB to Burrmill

Decided 2026-09-26. Gate 1 is taken as passed: parity 22/22 views byte-identical, 0.65x DuckDB
time-weighted and 22/22 within 1.5x on the 1,925-segment nest, 241-245 MB at 989,690 groups in a
Burrmill-only binary, and the footprint (495 MB querying test binary, 112 MB release, 4.76 GB
target) accepted by Chief as the price of taking DuckDB's C++ out of nuthatch. Build time is not a
win and is tracked separately in burrmill#7.

This file is the working plan for phases 2 and 3 of [plan.md](plan.md). Tick items here as they
land; the progress log carries the measurements.

## The fleet this has to survive

From muster, checked 2026-09-26. Every nest below serves something live.

| where | nest | version | read by | roll |
|---|---|---|---|---|
| Helsinki | `graph-allocations-nest-next` 8107 | 3.11.0 | Lodestar `/alloc`, `/horizon`, bare path; Foghorn; kittiwake | unit edit + restart |
| Helsinki | `graph-gns-nest-next` 8113 | 3.11.0 | Lodestar `/gns` | unit edit + restart |
| Helsinki | `nuthatch-dips` 8104 | 3.11.0 | Lodestar `/dips` | unit edit + restart |
| ThinkPad | `qos-reo-nest` 8124 | 3.11.0 | Helsinki `/qos` over the tailnet | unit edit + restart |
| ThinkPad | hosted platform, `nh-*` containers and pools | image `nuthatch:3.11.0` | platform users | image tag, many nests at once |
| ThinkPad | four Arcaidia hackathon nests | 3.6.1 | Max's dashboard | cannot roll in place (old decode registry); killswitch 2026-09-30 |

Rules that follow from it:

- **One nest at a time**, quiet ones first: DIPS, GNS, QoS, then alloc. The platform image last.
- **Stopped-store tar before every roll** that changes the engine. Roll-back is the previous binary in
  the unit plus that tar, exactly as today's runbook (`muster/runbooks.md`, "Roll a nest").
- **Shadow mode runs on real traffic**, replayed or live, never only on fixtures. Differences are
  classified, not hidden.
- **The 3.12.1 fixes (#1522, #1521) roll before any engine release** so a regression can be told
  from a pre-existing fault.
- No user query on DuckDB in a shipped binary once cutover lands (RFC-0042 §3a).

## Phase 2a: the engine trait, DuckDB the only implementation

Branch `pete/engine-trait` in nuthatch. A refactor that changes no answer: every test that passes
today passes unchanged. DuckDB reaches eleven source files, `analytics.rs` above all (connection,
spill directory, interrupt handles, `duckdb_views()` catalogue, `ToSql` binding, allowlist walk),
then `graft.rs`, `analytics_budget.rs`, `authored_entity_spike.rs`, `entities.rs`, `port_emit.rs`,
`dune_views.rs`, `analytics_scalars.rs`, `seal.rs`, `entity_offchain.rs`, `entity_lower.rs`.

- [x] Map every DuckDB touch point (2026-09-26). `tests/duckdb_containment.rs` already pins the
      boundary: nine source files import the crate, no DuckDB type crosses a `pub` signature, and
      six `pub(crate)` signatures carry one (`analytics_scalars::register`, five in `graft.rs`).
      The engine-shaped core is small: run SQL to rows with column names, one pooled session per
      nest with a content-hash invalidation key, a cross-thread interrupt, a memory/thread/spill
      budget, and the JSON encoder. Everything else is DuckDB-shaped: `json_serialize_sql` ASTs
      (security walk, reachability, graft identity, entity lowering, Dune), `EXPLAIN (FORMAT JSON)`
      operator names for RFC-0048, `duckdb_views/tables/functions`, `read_parquet(union_by_name)`
      with footer binding at DDL time, the `allowed_directories` sandbox, the Appender for hot rows,
      error-text matching (`segment_vanished`, `sql_errors.rs`), the seven `vscalar` UDFs under
      `graph`, and `version()` in graft identity.
- [x] Define the trait. Shape settled on 2026-09-26: it speaks nuthatch's nouns, not DDL. A
      `Session` binds a fact table from `(name, declared cols, sealed segment paths, hot rows,
      window)`, binds offchain snapshots and labels by path, defines an authored view by
      `(name, body)`, probes whether one segment binds, reads one file's schema, lists relations
      and view definitions, serialises a statement to an AST (DuckDB's JSON for now), counts cold
      scan operators for the admission bound, hands out an interrupt handle, and collects rows to
      JSON under a row and byte cap with the bind/execute failure split `Attempt` depends on. Flat
      files `src/engine.rs` and `src/engine_duck.rs`, because the containment scanner reads `src/`
      flat and a subdirectory would slip past it. `analytics.rs` keeps policy (guards, text gates,
      cache, deadline, sweep) and loses every `duckdb::` import; `engine_duck.rs` joins `KNOWN` and
      `analytics.rs` leaves it.
- [x] Written 2026-09-27 (nuthatch `src/engine.rs`): `Engine::{open, open_bare}` and `Session` with
      20 methods over `serde_json::Value`, `PathBuf` and arrow `RecordBatch`, nothing generic. `Died`
      and the interrupt handle moved with it.
- [x] Move DuckDB behind it (`src/engine_duck.rs`, 900 lines, the code moved verbatim): the opener
      and lockdown, the spill directory, the view DDL, the hot-row Appender, `json_serialize_sql`,
      `EXPLAIN` and its operator names, the catalogue functions, the encoder. `Session` is implemented
      on `duckdb::Connection` itself so tests keep their oracle connections; `DuckSession` adds the
      spill directory's lifetime. `analytics.rs` has no `duckdb::` outside its test module.
- [x] Every caller goes through the trait: `analytics.rs`'s public API is unchanged, so `/sql`,
      `/q/{name}`, `/explain`, the CLI, MCP and the seeds did not move. `FoldBinder` carries a
      `graft::Parser` for the parser role, which stays DuckDB's until phase 2 proper.
- [ ] The catalogue questions answered from `views/*.sql` and the registry where they can be. Not
      done: `has_relation` and `view_definitions` are trait methods the Burrmill session will answer
      from its own catalogue in 2b.
- [x] `tests/duckdb_containment.rs` passes with its pinned count still six: `engine_duck.rs` joins
      the known sites, three DuckDB-only tests moved into it, no new connection-typed `pub(crate)`
      signature.
- [x] Suite green on 2026-09-27: `cargo test --locked` as CI runs it, 1,836 passed and 0 failed
      across ten test binaries; with `folds`, analytics and engine_duck 124/124; `cargo fmt --check`
      and `cargo clippy --all-targets -D warnings` clean on both feature sets. No test changed except
      where it named a moved function. Committed as `5df8a00` and pushed on `pete/engine-trait`;
      the PR is Chief's to open.
- [ ] Release as an ordinary nuthatch release; roll it as one. Nothing in it is new behaviour.

## Phase 2b: shadow mode

Feature flag `shadow-burrmill`, off in release builds by default. Burrmill answers beside DuckDB;
DuckDB is served. It carries two engines and two Arrows, so the period is short.

**The dependency.** An earlier version of this paragraph weighed three ways for nuthatch to depend
on a private burrmill. The repository has been public throughout (`gh repo view` says so; the
`isPrivate: false` was misread on 2026-09-26, and the blog post carried "not public yet" for a
night before it was corrected). So it is a plain `git` dependency pinned to a revision, optional,
behind the `shadow-burrmill` feature; Cargo resolves it on every checkout and compiles it on none
that leave the feature off.

The `ShadowSession` itself does not depend on the choice: a `Session` that forwards every catalogue
call to both engines, serves the primary's rows, and runs the secondary afterwards under its own
permit. It can be built and tested with two DuckDB sessions and a planted difference before any
Burrmill session exists.

- [x] The pairing itself (2026-09-27, nuthatch `src/engine_shadow.rs` on `pete/shadow-session`,
      off `pete/engine-trait`): `ShadowEngine` opens a primary and a secondary session per nest;
      `ShadowSession` forwards every catalogue call to both, serves the primary's `collect`, compares
      the secondary's rows as a multiset, and records `Rows`, `Refusal`, `Catalogue` and `Skipped`
      differences through a sink (the `shadow` log target by default). The shadow runs inline and is
      skipped once the primary has used half the guard's budget; `Session::set_deadline` carries the
      deadline in. Five tests, with a planted one-row-short secondary and a past deadline. Nothing
      installs it yet: `engine_shadow::install` waits for the second engine.
- [x] Burrmill as that second engine (2026-09-27, nuthatch `src/engine_burrmill.rs`, feature
      `shadow-burrmill`, `git` dependency pinned to burrmill `b059dd0`): a `burrmill::Engine` opened
      empty per session, tables registered as the policy code binds them through burrmill's new
      `register_facts` (segment list, declared columns, hot rows as JSON, window) and
      `register_rows` (labels); rows cross as nuthatch JSON via `burrmill::df::encode`, never as
      arrow, since the two arrows differ (58 and 59). `enable_shadow()` installs it at `dev` start
      when the feature is on. Refused on purpose, and only ever asked of the primary: the parser
      role, the DuckDB plan walk, `view_definitions`, `query_arrow`. No cancellation handle yet: the
      budget rule and the primary's watchdog bound the request. One test seals a nest with
      nuthatch's own `seal_range`, stages hot rows, binds on both engines and runs three
      dashboard-shaped statements: no difference recorded. Clippy clean; the lockfile gained 64
      packages and changed no existing version.
- [x] Memory and a file (2026-09-27): each record carries the process RSS after each engine
      answered (a process-wide figure; the pair's difference is the shadow's cost), and
      `NUTHATCH_SHADOW_LOG=<path>` appends every record as a JSON line beside the log.
- [x] Cancellation (2026-09-27): `burrmill::Engine::cancel_token`, checked between output batches
      and inside every segment scan (`df/cancel.rs`), because a DataFusion aggregate over a join
      yields nothing above the scan until it is done (apache/datafusion#19358): a cross-join sum
      that ran 107 s past its cancel with the batch check alone stops within one scan batch with
      it. The Burrmill session's `interrupt_handle` is that token, so the watchdog and shutdown
      reach it as they reach DuckDB's.
- [ ] Classifier for expected differences, so the log holds only the unexplained. Done so far
      (2026-09-27): both-truncated compares nothing; `Unordered` for a `LIMIT` with no `ORDER BY`;
      `FloatOrder` for doubles equal to twelve significant digits; `Designed` for the checked
      rule's refusals and `Looser` for the no-ICU class (nuthatch `5b5ff81`, on main).
      `cold_velocity` and `/` need no class: the window is `//` now and `/` is DOUBLE on both
      (6.5). Left, and **deliberately not written ahead of a record** (2026-09-29): DuckDB 1.5.x
      wraps (duckdb#24081) and the cast-comparison bug
      (`docs/upstream/duckdb-cast-comparison-null-constant.md`). Both are DuckDB answering
      wrongly, no replay has met either, and a detector keyed on SQL text would explain away a
      Burrmill fault of the same shape. Name each from the first real record, on its values.
- [x] What the first replays found and fixed (2026-09-27, log entry "Shadow mode's first day"): a
      Burrmill `ORDER BY` alias bug (fixed, `dialect-parity` 210/210) and two dashboard statements
      summing `tokens_dec` (kittiwake `pete/dump-nest-sql` casts `tokens` instead; to merge before
      cutover). Run d: 22 views and 65 statements, no difference but one `FloatOrder`.
- [ ] Shadow never changes the served answer, its latency budget or its memory accounting: it runs
      after the DuckDB answer is sent, under its own permit, and is dropped if the guard is near.
- [x] Replay harness (2026-09-27): `shadow_replay_over_a_nest` in nuthatch, ignored unless
      `NUTHATCH_SHADOW_NEST` names a nest; installs the shadow as `dev` does and reads every
      authored view whole through `query_guarded`, the production path from the text gates to the
      collect. With `NUTHATCH_SHADOW_SQL` it also runs the dashboard's own statements: 81 of them,
      generated from kittiwake's SQL functions by `crates/read/examples/dump_nest_sql.rs` (branch
      `pete/dump-nest-sql`) with marker ids, kept as `docs/bench/dashboard-statements-2026-09-27.sql`
      and resolved to real ids from the nest at run time. The platform's statements are still to
      be captured.
- [x] Shadow on the ThinkPad copy, first runs (2026-09-27, `docs/bench/shadow-replay-thinkpad-*`):
      run a found 5 differences in 22 views, all one fault of the harness (the primary truncated at
      the 64 MiB byte cap, the Burrmill session applied only the row cap); the cap moved into
      `collect`'s contract and both-truncated compares nothing. Run b: **22 views, 0 differences**.
- [x] Merged to nuthatch main as #1527 (`711ae88`, 2026-09-28), with Burrmill bounded by
      `analytics.memory_limit` and the cursor budget counting both engines (Jules: ship, 88/100).
      A shadow build therefore needs `NUTHATCH_SQL_MAX_CONCURRENCY=1` or a smaller memory limit.
- [ ] Then Helsinki DIPS, then GNS, each for a release cycle. Sealed data measured 2026-09-28:
      every view identical and within 64 MB on both (progress log, "DIPS and GNS fit").
      **DIPS on `3.12.1-shadow.1` since 2026-09-28 12:11 UTC** (nuthatch main `711ae88`), one SQL
      permit, `MemoryHigh=2G`; two faults in the prepared roll fixed first (two permits breach the
      two-engine budget; `ProtectSystem=strict` made the log read-only). The log's first record is a
      planted `printf` refusal, not traffic; it also names a real gap: Burrmill has no `printf`.
      **2026-10-01:** still one line, and no evidence either way: the shadow recorded differences
      only, so an empty log is also what a nest nobody queried writes. It counts every statement
      now (`kind: "Tally"` lines, nuthatch `5ffed4f`), on the stack merged with main (`34fc981`).
      The cycle restarts from a shadow build of that, with a current Burrmill; `711ae88`'s predates
      every fix since 2026-09-28.
- [x] Concurrency sweep on the DataFusion path at 32 clients on the nest it will serve (2026-09-29,
      progress log): DIPS and GNS through the real `/sql` path, Burrmill alone level with or ahead of
      DuckDB (355 qps, p99 103-122 ms at 32 clients, no 503s, ~100 MB). It also found GNS's
      `developer_activity_weekly` refused by production DuckDB for want of ICU.
- [ ] Joins and cancellation: the scan-level token above is the mechanism. A cancelled join frees
      its memory (2026-09-29, `a_cancelled_join_returns_its_memory`: the pool is at zero within
      milliseconds), though it runs on for one input batch past the cancel, bounded by the pool.
      Still owed: the per-query timeout at the 30 s guard for the cutover build (the shadow build has
      DuckDB's watchdog in front). **2026-09-29:** the guard did not stop Burrmill on a recursive CTE
      or a nested-loop join over `range`, both past 120 s against a 250 ms budget (nuthatch
      `pete/burrmill-watchdog`). Fixed in Burrmill: the caller races the token, and `Cancellable`
      checks it in every join's output and filter (progress log); the nuthatch test passes on both
      engines with the rev bumped. `RangeJoinExec` since: resumable probe, build in the pool.

**Gate 2** (all four, or no cutover):

- [ ] A release cycle of real nest traffic with zero unexplained differences.
- [ ] p99 within the `/sql` budget (30 s, 2 permits) on every shadowed nest.
- [ ] Memory within nuthatch's RFC-0047 envelope with both engines resident, and Burrmill alone under the
      nest's `MemoryHigh`.
      **Not met at DuckDB's figure (2026-09-27):** bounded to the same 512 MB, Burrmill refuses 24
      of the replay's statements that DuckDB answers; 10 at 1 GB, 2 at 2 GB. DataFusion's hash join
      and final aggregate cannot spill (progress log, "Shadow mode under DuckDB's memory limit").
- [ ] `burrmill::inspect::reach` at least as strict as `json_serialize_sql` on the security corpus
      (`reach-parity` 38/46 identical, 8 stricter, 0 looser today; the 8 documented). Measured live
      since 2026-09-27: the shadow runs both walks on every statement and counts `ParserLooser`;
      run g over the 22 views and 81 dashboard statements had none of either kind.

## Everything DuckDB does, and what replaces it (2026-09-28)

Chief, 2026-09-28: "burrmill takes over EVERYTHING and we remove duckdb". No DuckDB behind a
feature either, folds and the parser role included; DuckDB may stay only as a dev-dependency oracle
until phase 3b takes it out of `Cargo.toml` too. Inventory from nuthatch main `711ae88`; `duckdb` is
an unconditional dependency (`bundled`, `parquet`, `json`), and `graph` adds `vscalar`.

- [x] **Query execution** (`Session::collect` and friends): Burrmill in the shadow.
- [x] **Sealed segment binding** (`bind_facts`, union by name, declared columns, window).
- [x] **Hot rows** (`load_hot`), **offchain snapshots and labels** (`bind_snapshots`, `bind_labels`).
- [x] **Lockdown** (2026-09-28): `open_empty_budgeted` takes memory, threads (the runtime's workers
      too) and a spill directory from nuthatch's `new_spill_dir` capped by `max_temp_size`; the sort
      merge reservation scales with the budget. File confinement holds by construction: Burrmill
      reads only the files it is handed. nuthatch `pete/burrmill-budget`.
- [x] **Catalogue** (2026-09-28): the Burrmill session records view definitions, and
      `Session::table_refs` answers the sweep's and admission's walks from `inspect::base_tables`
      (CTE scope as nuthatch's walk) and `inspect::refs`; `refs-parity` identical on all 31 authored
      views of allocations, GNS and DIPS.
- [x] **Scalars** (2026-09-28): `Engine::register_text_function`; `graph` builds register the seven
      `nuthatch_*` functions on Burrmill over the same `evaluate`, compared against DuckDB in a test.
- [x] **EXPLAIN admission** (RFC-0048, 2026-09-28): `Engine::parquet_scans` walks DataFusion's
      physical plan, counts Parquet file scans (the owned fold counts one), allows the ordinary
      operators by name and refuses nested-loop joins, recursive queries and anything unknown;
      nuthatch `pete/burrmill-admission`. `scan-parity` against DuckDB's `EXPLAIN` walk: DIPS 2/2
      identical, GNS 6/7 identical and one lower, allocations 9/22 identical, 4 lower, and **9 that
      DuckDB cannot bound** (its plans rescan through `NESTED_LOOP_JOIN` and `LEFT_DELIM_JOIN`) which
      Burrmill bounds, because DataFusion decorrelates them into hash joins. Never stricter. So a
      named query over `lodestar_indexer_ledger`, `lodestar_indexers`, `lodestar_network`, the
      delegator views or the daily views, refused as unboundable on DuckDB today, is admitted on
      Burrmill with a bound from Burrmill's own plan: a behaviour change for the release notes.
- [ ] **Parser role** (the long pole): `/sql` allowlist walk, `table_refs_in`/`expand_through_views`,
      FoldBinder, graft `canonical_plan`, entities (`plan_ast`, `validate_sql`, `aggregates_among`),
      `entity_lower`, `dune_views` all read DuckDB's `json_serialize_sql` shape. Port each to
      sqlparser's AST (`burrmill::inspect::reach` already does). Graft reuse keys and fold identities
      hash that output plus `SELECT version()`: a deliberate re-key, `CACHE_FORMAT_VERSION` bumped.
      **Graft done** (2026-09-28, nuthatch `pete/parser-graft`): `Session::canonical_plan` and
      `engine_version`, DuckDB's as before, Burrmill's from `inspect::canonical` and
      `burrmill::ENGINE` (a hash of Burrmill's source and manifest). The key re-keys when the engine
      changes, by the version field, with no hand-bumped constant. `duckdb_containment` pinned 6 → 5.
      Fold binder done (2026-09-28, `pete/parser-folds`). **Entities and entity lowering done**
      (2026-09-28, `pete/parser-entities-port`, merged into `pete/burrmill-folds`): gate and lowering
      on sqlparser, the DuckDB versions kept as test oracles, identical on 335 inputs; eight
      constructs sqlparser cannot read are now refused (`1_000`, `E'x'`, `ISNULL`, `**`,
      `GROUP BY #1`, `GLOB`, `TABLE t`, `ASOF JOIN`, the last one lowered wrongly as an inner join
      before). **The parser role is off DuckDB** in every production path; graft's remaining DuckDB
      code is test-only. A live bug the port found and kept for parity: entity lowering ignores an
      aggregate's `FILTER (WHERE …)`, so `SUM(b) FILTER (WHERE b > 0)` is maintained as `SUM(b)`;
      the gate should refuse it, as a separate fix. **Dune is not ported**:
      Chief, 2026-09-28, Dune support will be deprecated, so `dune_views.rs` and `nuthatch emit dune`
      leave with DuckDB instead.
- [x] **Folds** (RFC-0059, 2026-09-28): Burrmill host tables (`create_table_as`, `drop_relation`,
      `begin`/`commit`/`rollback`, `write_parquet`/`load_parquet`, `describe` in DuckDB's type names,
      `sql_ipc`); nuthatch `pete/burrmill-folds` maps the fold runtime's statements onto them, moves
      checkpoint write and resume behind `Session`, and reads `query_arrow` through IPC, so the arrow
      versions need not meet. All 53 fold tests pass with Burrmill as the engine (a test-only
      override). They caught two faults: graft's static refusals read DuckDB's JSON and went silent
      on Burrmill (now `graft::refusals_in_sql` over sqlparser, identical on 23 cases), and a
      `HUGEINT` carry passed as `DECIMAL(38,0)` (now refused by its declared name).
- [ ] **DuckDB-dialect SQL nuthatch generates**: `HUGEINT`/`UBIGINT`/`TRY_CAST` (analytics, recipes,
      views, webhooks), GraphQL's `struct_pack`/`to_json(list())`, port emit's output, the
      `FORBIDDEN_FNS` denylist and error-text matching (`sql_errors.rs`). Each checked on Burrmill.
      **Measured 2026-09-29** (nuthatch `pete/burrmill-dialect`, test
      `duckdb_and_burrmill_agree_on_the_sql_nuthatch_generates`: the four restart folds, four
      recipes, a webhook predicate and six GraphQL shapes through the shadow). Found and fixed:
      the restart folds and the balance recipes summed `TRY_CAST(v AS HUGEINT)`, which Burrmill's
      checked rule refuses or sums exactly, so one transfer past i128 stopped a restart re-seeding
      the balance, exposure and velocity views; they now write the drop as
      `CASE WHEN TRY_CAST(v AS HUGEINT) IS NOT NULL THEN CAST(v AS HUGEINT) END`, the same answer
      on DuckDB (rows and `COUNT(*)` unchanged). Port emit's entity totals had the same shape in
      `DECIMAL(38,0)` and take the same spelling (`analytics::cast_or_null`); their `_overflow`
      flag is untouched. GraphQL's derived lists needed three things of
      Burrmill: `struct_pack` (to `named_struct`), `to_json` in DuckDB's spelling (upper-case `\u`
      escapes, SQL NULL for NULL), and a correlated `ORDER BY … LIMIT` inside a scalar subquery,
      which DataFusion cannot decorrelate: ranked per inner key instead (`dialect.rs`,
      `top_n_correlated`). `dialect-parity` 224/224. A spill past `max_temp_size` now names
      `max_temp_directory_size` as DuckDB does, so nuthatch's hint names the right setting.
      `FORBIDDEN_FNS` needs nothing: Burrmill resolves only `range` and `generate_series` as table
      functions and refuses `COPY`, so the denylist is a second lock. **Open:** Burrmill's HUGEINT
      is `DECIMAL(38,0)`, so a value in `[10^38, 2^127)` is NULL to its `TRY_CAST` and a value to
      DuckDB's; nuthatch's own `_dec`/`_overflow` columns already draw the line at 38 digits while
      the folds and the IVM circuit draw it at i128. **Decided 2026-09-29: 38 digits everywhere**
      (nuthatch `pete/transfer-38-digits`, off `pete/burrmill-dialect`): `views::transfer_value` for
      the live views, `analytics::exact_or_null` for the folds and recipes, the band dropped by both
      engines and counted in `dropped_over_i128`, which keeps its name. A behaviour change for the
      release notes: a transfer in `[10^38, 2^127)` leaves balances, exposure and velocity. `printf` and `format` added
      (`df/printf.rs`), a subset of fmt's specs measured against DuckDB, including its
      refusals (`%s` of a number, `%f` of an integer), `%d` of a boolean and `%x` of a negative
      as 64-bit two's complement; `dialect-parity` 239/239.
- [ ] **Allocations nest memory, measured 2026-10-01** (progress log): 2 GB refuses on every run at
      the default two analytics threads and on none of six at eight, so the nest takes
      `NUTHATCH_ANALYTICS_THREADS=8` with the 2 GB. Earlier:
      ordered scans, the chain-order window rule, spilling and the view
      rewrites (`ledger-windows.md`) landed; `lodestar_delegator_stakes` still at 1 GB.
      **Decided 2026-09-29 (Chief): raise the figure, no spilling work.** `analytics.memory_limit`
      about 2 GB for the allocations nest and its MemoryHigh above nuthatch's RFC-0047 shipped 2 GiB split, so
      an amendment to nuthatch's RFC-0047; the figure from the replay's per-view minimums, set with that nest's roll.
- [x] **Aggregates over an overflowed `_dec`** (decided 2026-09-29, Chief): refused on Burrmill, as
      its checked rule does, not answered from the values that fit as DuckDB does. A release-note
      item. The four tests that pinned DuckDB's answer are the cutover's to update
      (`test-inventory.md`).
- [ ] **Tests**: ~45 DuckDB-oracle tests in `src/` and `tests/` moved or retired; the Trino contract
      compared against Burrmill; `duckdb_containment.rs` shrunk to zero and deleted.
- [ ] **Packaging**: `authored_entity_spike.rs` deleted, `tools/*` DuckDB deps, CI BOM scripts.

Order: small items and the arrow alignment, the parser role, folds, EXPLAIN admission, allocations
memory. DIPS and GNS can cut over once the parser role and admission are Burrmill's; they need
neither folds nor the ledger work. Estimate on 2026-09-28: about six weeks of work to DuckDB out of
`Cargo.toml`, gated in calendar by one clean shadow release cycle per nest.

**Found on the way, not caused by this work:** with `graph` and `shadow-burrmill` together, which
CI never builds, two tests fail on nuthatch `main` since #1527: `e2e_trino_contract` (the five views
listed in another order) and `authoring_eval_board` (sealing slower than its 120 s wait). DIPS runs
the plain shadow build, not `graph`. On `pete/burrmill-folds` with the same features (2026-09-29):
`entity_lower::the_port_lowers_every_statement_exactly_as_duckdbs_parse_did` fails on the key order
of a JSON object printed inside two error messages (Burrmill unifies `serde_json/preserve_order`
in), the answers agreeing; `readers_racing_folds_never_answer_short_and_leave_no_file_behind` failed
once and passed on rerun. `graft::canonical_plan` is dead outside tests there and fails
`clippy -D warnings`; `pete/burrmill-dialect` carries the `#[cfg(test)]`.

## The network nest (RFC-0060), found 2026-09-30

Every earlier flip built `shadow-burrmill` alone; the network nest's tests need `graph`, so none of
them had run on Burrmill. With `graph` they did: 21 network-contract tests and 6 of `serve`'s
network documents, 27 failures. Fixed since in Burrmill (`938d52b`, `e9fc080`): `BIGNUM` as
`DECIMAL(38,0)` (166 uses over 15 view files), `AS MATERIALIZED`, a recursive query tainted only by
what it casts, `regexp_full_match`, `hex`, and a CTE with a column list repeating an expression.
The rest:

- [x] **Correlated subqueries** DataFusion does not flatten: "the latest matching event per row",
      as a lateral join and as a scalar subquery (`LatestCorrelation`, `8f7c2c3`, `c5e49b7`); a
      to-one lookup that is not aggregated, alone and inside a paged list (`SingleRowSubqueries`,
      `2997998`).
- [x] **`SUM` over a `_dec` whose source the checked rule could not trace**: the taint started at a
      lateral derived table and ran through a recursive fold (`bbbeace`, `ff9eb30`).
- [x] **An empty scalar subquery under `coalesce`** came back NULL: DataFusion types the subquery by
      its column (`e747729`).
- [x] **`>>` and `<<`**, refused by the checked rule, now DuckDB's (`2997998`).
- [x] **Past 38 digits.** Decided 2026-09-30 (Chief): `BIGNUM` stays 38 digits, refusing past them.
      The 2^256-1 values that reach arithmetic in the fixtures were balances and deltas, not
      sentinels; on Chief's second call the three fixtures keep exact arithmetic at the 38-digit line
      (nuthatch `pete/burrmill-watchdog`). Real GRT amounts sit ten orders of magnitude below it.

With Burrmill the engine: network contract 21/21 and `serve::tests` 97/97; the contract 21/21 on
DuckDB too. Owed: the contract takes 116 s on Burrmill against 32 s on DuckDB.

## Phase 3a: cutover

- [x] **The switch** (2026-09-30, nuthatch `pete/burrmill-watchdog`): `NUTHATCH_ENGINE` =
      `duckdb` | `shadow` | `burrmill` in a `shadow-burrmill` build, read by `dev` at startup; unset,
      each build does what it did (shadow, or DuckDB), and a build without Burrmill refuses to be
      asked for it. So a nest moves to Burrmill, and back, with a unit edit and a restart, no new
      binary. Checked on a DIPS copy: the same statement answered by each engine by its own error
      text, the startup log naming it.
- [ ] Burrmill the default engine; DuckDB a dev-dependency oracle only, gone from the shipped binary.
- [ ] The parser role (`reach`, graft canonical form, entity gate, Dune, lowering) off
      `json_serialize_sql`.
- [ ] `NUTHATCH_ANALYTICS_MEMORY_LIMIT`, `NUTHATCH_SQL_MAX_CONCURRENCY` and the spill settings keep
      their names and meanings on the new engine.
- [ ] Docker image built without a C++ toolchain; image size recorded.
- [ ] Roll: stopped-store tar, then DIPS. Watch a day. GNS. QoS. Alloc, with Lodestar's crons watched
      through one full cycle. Platform image last, pools first, then per-nest containers.
- [ ] Roll-back rehearsed once on the ThinkPad before the first Helsinki roll.
      With the switch, roll-back is `NUTHATCH_ENGINE=shadow` (or `duckdb`) and a restart.

**Changed 2026-10-01 (Chief):** no shadow release cycle per nest. Each nest rolls to
`NUTHATCH_ENGINE=checked` (Burrmill served, DuckDB checking, nuthatch#1615) as soon as the build
exists, DIPS, GNS, QoS, allocations, platform; DuckDB checks for about a month and leaves
`Cargo.toml` around early November. Gate 2 is read from the checked log's differences and `Tally`
lines instead. The public account is nuthatch-indexer.com
`/blog/switching-the-engine-under-a-live-indexer`.

**Changed again 2026-10-01 (Chief):** no month either. The live subgraphs are the judge, a nest
rolls on `lodestar-parity.sh` before and after, and DuckDB leaves as soon as every nest runs
without it. DIPS, GNS, allocations and QoS were served by Burrmill by 18:46 UTC that day.

## Phase 3b: removal

- [ ] One clean release on Burrmill across the fleet.
- [ ] `duckdb` out of `Cargo.toml`, `deny.toml` and the Dockerfile. The generated corpus, the `.slt`
      files and the reference oracle stay as the regression suite. **Written 2026-10-02**
      (nuthatch#1626, not merged): out of the manifest and lockfile, the engine and shadow files,
      Dune and the spike deleted, 1,880 and 1,989 tests passing on the ThinkPad. The Dockerfile
      never had a toolchain (it copies the release binary); the binary no longer links libstdc++.
- [ ] Footprint and build time re-measured on nuthatch itself (burrmill#7's consumer half,
      nuthatch#1428).
- [ ] muster updated: versions, the runbook's roll and roll-back, and the `.duckdb/` line under
      "Nests".

## Not in this plan

- Hackathon nests: retire on schedule, never migrate.
- DataFusion as a cold-path fallback, JIT, format changes, crates.io (ROADMAP, "Not on this roadmap").
