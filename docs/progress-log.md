# Progress log

Newest first. One entry per RFC-0044 slice.

---

## Catch-up, 2026-10-02 to 2026-11-11

Written on 2026-11-11 from the merged PRs; each figure is the PR's own, and the PR has its method.
Nothing here was re-measured for this entry.

**The browser tab and ingest, 2026-10-16 to 11-11.** The engine runs on `wasm32-unknown-unknown`:
tokio native-only, an async query path driven by the caller's event loop, an engine opened from an
object store, partitions coalesced in turn rather than spawned (#106, #109, #111). The Lodestar
nest's 23 views answer in a tab identically to native; over the 22 DuckDB-Wasm also answered, 43.6 s
cold against its 158.8. `burrmill-ingest` turns an untrusted CSV into Arrow under counted budgets,
and `burrmill-wasm` with `demo/` asks a model about a dropped file in schema, aggregates,
pseudonymised or local-only mode (#128): reader 128 KB gzipped, reader plus engine 7.4 MB against
the 8 MiB gate, which CI now enforces at 7,473,082 bytes (#192). Ingest then stopped changing
values silently (#195), counted empty fields, rows and built columns against its budget (#196), and
sniffed a split character and delimiters linearly (#208). The cold audit of 2026-11-10 filed #129 to
#190; four p0s are privacy-mode leaks on the page (#129, #130, #131, #133), open.

**Memory, 2026-10-03 to 10-31.** Every holding operator is budgeted over string views (#45); a
window over its whole input charges the pool (#49); a cross join's collected side is charged for its
rows (#54); scans charge their per-segment bookkeeping (#87); `SharedPool` lets engines share one
bound (#65); a shared subquery too big to hold runs fresh per reader (#113). Computed values are
bounded at 16 MiB each and large ones charged to the pool (#122, `Engine::set_value_limit`).

**Speed, 2026-10-04 to 11-05.** `RangeBounds` bounds a join's probe scan by the build side's
extremes: the QoS kittiwake one-day statements 7 to 11 s to 1.1 to 1.6 s, 47 of 5.32K row groups
read (#68, nuthatch#1800). The budgeted batch went from 128 rows to 2,048: `lodestar_indexer_ledger`
2.7 to 1.43 s, `lodestar_indexers` 3.4 to 1.95 s at 512 MB and two threads (#112). Top-k and join
dynamic filters reach the scan (#61, #73, #80). Sharing planned once and `NarrowJoins` for BetSwirl's
`bets` pages, about 3.0 to 2.0 s (#93, #94). The footer cache is read at last (#101), repeated
windows computed once (#103), running max and min in one pass (#107), addresses kept as fixed binary
to the answer (#116 to #118, #205). Wide nests read footer schemas once per process: ENS
`information_schema.tables` 5.6 to 7.0 s each to 1.32 to 1.42 s after the first, 856 to 456 MB
(#127).

**Answers, 2026-10-02 to 11-11.** Exact `SUM` and `AVG` over `DOUBLE`, correctly rounded whatever
the partitioning (#77, nuthatch#1849), then made cheaper (#84, #85). DuckDB's `//` over `DECIMAL`,
`ORDER BY` alias expressions, `%` by zero, `string_agg` order, `LIKE` escapes and decimal spellings
(#37, #39, #71); `TRY_CAST` to `HUGEINT` past 38 digits refuses the value (#59, #72); a view's
`ORDER BY` orders the answer when nothing above reorders (#95).

**Safety and the host surface.** A statement deep enough to abort the planner refuses (#25); the
hidden parts of a registration are refused whatever comes before (#29, #204); `EXPLAIN` is checked
as the statement it wraps (#32); `levenshtein` refuses past a bounded amount of work (#198); every
statement runs on its own 8 MiB thread (#123, #207). Metered statements report what they selected,
read, held and spent in scan CPU (#120, #121). CI from #35, with cargo deny, read-only jobs and a
pinned toolchain (#206).

**Designs.** RFC-0046, partial state over sealed segments, nothing built (#104).

---

## Where the 195 MB is, and what three profile changes do to it — 2026-10-02

The measurements behind [RFC-0045](rfc/RFC-0045-one-engine-smaller.md). Published artifacts by
`objdump -h`: 4.0.2 to 4.1.0 is `.text` 82.5 to 147.1 MB, unwinding tables 12.3 to 26.8, file
108.5 to 195.0. A symbolled host build of nuthatch `79d3180` on this crate at `17e0a22` (aarch64
macOS, the shipped `lto = "thin"` profile, 124.6 MB text, 213.8 MB file, 268,478 functions), every
function attributed to the first non-std crate its symbol names: sqlparser 24.6 MiB, std 20.6,
datafusion 19.2 over 19 crates, arrow 15.6 in two versions, wasmtime and cranelift 9.6, nuthatch 4.2,
dbsp 3.5, parquet 3.3, stacker 2.9, burrmill 0.8. sqlparser is 0.85 MiB of parser and 23 MiB of
derived `Hash` (6.3), `drop_in_place` (5.7), `Visit` (3.7), `PartialEq` (3.4), `VisitMut` (1.9) and
`Clone` (1.5) over the whole grammar; `<Statement as Hash>::hash` is in the binary 45 times and
`::eq` 55 times at 11 to 26 KB a copy. The graph holds arrow 58 and 59, parquet 58 and 59,
object_store 0.12 and 0.13; this crate's own `object_store = "0.12"` is the odd one against
DataFusion 55.

One change at a time, manifest restored after each:

| build | text | file | functions | build |
|---|---:|---:|---:|---:|
| shipped | 124.6 MB | 213.8 MB | 268,478 | 224 s |
| nuthatch on arrow 59, parquet 59 (no code change) | 120.6 | 207.2 | 259,921 | |
| `lto = "fat"`, `codegen-units = 1` | 91.1 | 145.9 | 137,126 | 778 s |
| the same, sqlparser at `opt-level = "z"` | 89.3 | 145.2 | 144,070 | 748 s |

Fat LTO halves sqlparser (12.5 MiB) and leaves eleven copies of `Statement::hash`. A link map
(`-Wl,-map`) puts the copies in the codegen units of `datafusion-expr`, `datafusion-optimizer`,
`datafusion-functions` and burrmill, six per unit: they are instantiated by hashing DataFusion's
`Expr`, whose `Wildcard` variant carries sqlparser's `ReplaceSelectElement` and through it a
sqlparser `Expr`, `Query` and `Statement`. DataFusion's shims for those types sit behind
`datafusion-expr`'s `sql` feature, which `datafusion-sql` requires on; the fix is an upstream patch. Query time under the fat profile is unmeasured and is S1's gate. cargo-bloat's
own crate table under thin LTO put sqlparser at 13.7 MiB while its function filter found 1.5, so
neither of its numbers is quoted.

---

## 4.1.0 released and on every nest — 2026-10-02

Tagged at `79d3180` (#1626, #1627 notes and version, #1628 jemalloc) after two tags on the wrong
commit: the tag command moves the tag to main, and main had not been merged to. Release run
36993133979: notes, both targets, container image, published. Linux artifact: 195,005,312 bytes,
`libc`/`libm`/`libgcc` only, jemalloc in, **glibc floor `GLIBC_2.35`** (`hypot`, `hypotf`), where
4.0.2 was 2.34; nuthatch#1649 corrects the README, install guide, notes and the pinning test, and
names RHEL 9 and Amazon Linux 2023 as needing the source build.

Rolled with muster's `roll-fleet-release.sh`: Helsinki's five units at about 11:00 UTC, the QoS
nest at 11:14, every one ready on first start with no restarts, `typeof(1)` answering `BIGINT`.
RSS after the roll: DIPS 33 MB, GNS 112 MB, allocations 1,135 MB; QoS 951 MB eight minutes in
(the glibc build was past 4 GB at that age and 9.5 GB when replaced). Lodestar healthy, no
failing jobs.

**State of the migration.** Six nests, one engine, no DuckDB in any binary. Open: the public log
(nuthatch-frontend#68), the retrospective post, #1649, muster uncommitted. Engine work owed: day
pruning through a one-row join side (QoS statements 4.5 s against DuckDB's 0.7), planning cost on
deep views (the graph CI job at 37 to 45 minutes), spilling joins, and the seven fee epochs on the
allocations nest that are the views' and not the engine's.

---

## Every nest on Burrmill, and the QoS nest's memory — 2026-10-02

**The fleet.** Chief merged nuthatch#1626 at 10:40 UTC through muster's `finish-migration.sh`;
`data-services-nest` and the legacy archive had rolled to `4.1.0-pre.1` at 07:37 (24 of 24
recorded answers unchanged), and DIPS went from `checked` to `burrmill`. No nest consults DuckDB.
`v4.1.0` was tagged before #1627 (notes, version bump) merged, so the release run failed on its
missing notes and published nothing; the tag is moved after the merge.

**The QoS nest** (ThinkPad, Burrmill alone since 2026-10-01 18:46) was at 7.3 GB resident plus
4.3 GB swapped by morning, throttled by its 6G/7G cgroup and not answering; kittiwake's
`ingest-qos-days` failed 20 times overnight. Restarted 07:43 UTC, it was back at 4.7 GB within
40 minutes with one engine session and 229 statements. Reproduced on a copy with kittiwake's own
mix (55 indexers x the warmer's four, then the ingest job's four whole-day statements, two clients):

| allocator | after 170 statements | after all 279 |
|---|---:|---:|
| glibc | 2.8 GB | **5.6 GB** |
| glibc, `MALLOC_ARENA_MAX=2` | 2.7 GB (at 130) | |
| jemalloc | 2.0 GB | under 2.1 GB |

The four whole-day statements alone, fresh server, 2 GB pool: glibc 1.72 GB afterwards and the same
twenty seconds later; jemalloc 1.28 GB, then 0.95 GB. **The engine's limit holds; glibc keeps what a
large statement freed**, and the memory is outside DataFusion's pool (decoded pages, the row
encoding, the JSON answer). nuthatch#1628 makes jemalloc the global allocator on Linux. Until it is
on the nest, its cgroup is at 14G/16G on a 62 GB box.

**Also measured:** DataFusion's debug-build invariant checks are not why the router tests time out
unoptimised; the stacks sit in physical planning (`equivalence::properties`, `Statistics`).

---

## DuckDB out of nuthatch's tree — 2026-10-02

nuthatch#1626 (`pete/duckdb-removal`, three commits on main): `duckdb` is out of `Cargo.toml` and
the lockfile, Burrmill is a plain dependency pinned at `17e0a22`, and the `shadow-burrmill` feature
is gone. 80 files, 971 lines in and 22,639 out. Not merged; Chief merges.

**What Burrmill as the default showed** (1,948 passed and 7 failed before any fix):

- `typeof` did not exist. Added here (`17e0a22`): the type as DuckDB names it, `"NULL"` quoted as
  DuckDB quotes it. A `HUGEINT` is carried as `DECIMAL(38,0)` and is named so, which nuthatch's two
  tests now expect. A bare `1` is `BIGINT` here and `INTEGER` in DuckDB: listed in `KNOWN`.
  dialect-parity 273/273.
- A maintained relation lost its declared types: `load_relation` staged every column as text. It
  now validates each row against the plan's type as DuckDB's appender did, registers the rows as
  text under `<name>__raw` and a view under the name that casts each column, with a marker column
  so an empty relation exists with no rows. `drop_relation` is implemented, so a pooled session
  stops answering for an entity that has since faulted.
- An empty answer had no column names: `collect` read them from the first batch. It asks
  `describe` when there was none.
- A statement past the spill cap was stopped by DataFusion's disk manager itself, the whole test
  over in 0.28 s, with a message naming `datafusion.runtime.max_temp_directory_size`.
  nuthatch answers that as its own `QuerySpillExceeded`, so `/sql` still says 507.

**What went with DuckDB:** `engine_duck.rs` (1,267 lines), `engine_shadow.rs` (1,074), the shadow
and checked modes and their replay test, `nuthatch emit dune` with `dune_views.rs` and
`dune_emit.rs` (Chief, 2026-09-28: Dune support is deprecated), the RFC-0041 spike and
`bench authored-entity`, the two `duck_oracle` test modules, `tools/df-gate`,
`tests/duckdb_containment.rs` and `duckdb_extensions_are_static.rs`. `NUTHATCH_ENGINE=duckdb`,
`shadow` or `checked` is refused at startup; unset or `burrmill` starts. DIPS runs `checked` and
needs that line changed before it takes the build. One `tools/slice6` crate still names `duckdb`
in its own manifest: it is outside the workspace and unbuilt, and it stays because its raw logs
carry terminal escape bytes, which `gh pr diff` refuses to print and Jules therefore cannot review.

**Two things that passed and should not have.** `tests/abi_floors_documented.rs` required the
README to state a libstdc++ floor "because it embeds DuckDB", and went on passing with DuckDB
gone, because the README still said so. The binary links `libc`, `libm` and `libgcc` now (`ldd`
on the ThinkPad build); README, `install.md` and the test say that. And
`tests/engine_batch_boundary.rs` sized its cases at DuckDB's 2,048-row vector, 5,000 at most, so
on DataFusion's 8,192-row batches no case crossed a boundary. Resized to 8,191, 8,192, 8,193 and
20,000, the grouped case to 16,391. All pass.

**Suite on the ThinkPad**, release, `TZ=UTC`: 1,880 passed on default features and 1,989 with
`graph`, none failing (each assembled from a full run and a rerun of the one documentation check
whose allowlist count was wrong). `clippy --all-targets -D warnings` clean under default, `graph`,
`folds`, `counter`, `exex` and `postgres-store`. Not run here: the Postgres, MinIO and Trino jobs,
and the footprint jobs, which CI runs and which measure a Burrmill binary for the first time.

**What CI found that the ThinkPad could not.** CI tests an unoptimised build; I had run release.
The graph job failed two router tests of the network endpoint on "exceeded the 29s time budget".
Each of those tests takes 42 and 47 s alone in a release build on Burrmill, every query planning
through about thirty views; unoptimised on four cores one query passes both the 30 s budget and
the fixture's 60 s freshness limit. Measured before choosing, on the ThinkPad, `-c 0-3` to stand
in for a runner (its unoptimised library suite took 211 s there against CI's 255):

| dependencies in the test profile | build, CPU-minutes | the two tests alone |
|---|---:|---|
| unoptimised | 14 | both fail at 29 s |
| all at `opt-level = 1` | 75 | 67 s, 60 s |
| all at `opt-level = 2` | 113 | 63 s, 54 s |
| Burrmill, DataFusion, sqlparser at 2 | 52 | 84 s, 76 s; one still fails on four cores |
| the same, `debug-assertions = false`, at 1 | 31 | 95 s, 95 s (with the allowance below) |
| release | | 47 s, 42 s |

No profile setting buys enough, and DataFusion's per-rule invariant check in debug builds, which
I suspected, is not the bulk of it. The stacks (gdb, six samples) sit in physical planning:
`equivalence::properties`, `Statistics`, `ChildStats`. So the router tests take ten times the
budget under `cfg!(test)` and their fixtures a 600 s freshness limit, as
`tests/network_contract.rs` already allows itself for the same views. With that the whole `graph`
suite passes unoptimised on four cores: 1,989, in 21 minutes of which the network contract is 646 s.
On a GitHub runner the same job took 45 minutes (library tests 670 s, `network_contract`
1,183 s) where it was 13 on DuckDB; its ceiling is raised from 60 to 90. All seventeen checks pass.
**This is the planning cost on deep views, and it is the engine's
to fix**: DataFusion 56's `EnsureRequirements` change, and whatever is left after it.

**Three runs were lost** to rsyncing a newer tree under a build still in its second pass; the
failures belonged to no tree. Memory note `rsync-under-a-running-build`.

**The last two nests, replayed 2026-10-02.** `graph-staking-legacy-history` (102 MB, 58 segments,
`serve` on 3.8.4) and `data-services-nest` (2.4 MB, `dev` on 3.12.1) were copied to the ThinkPad
and served there by `4.1.0-pre.1`, a release build of #1626's head (192 MB, sha256 `bf0bffb5…`,
no libstdc++). Neither has an authored view, so the test is Lodestar's statements: kittiwake's
`daily_flow_sql` over four ranges, counts and bounds of each table and a grouped sum for the
archive; `registry_sql` and counts for each of the five data services. The same 24 statements
were asked of the live nests on DuckDB through their own `/sql`. **24 of 24 agree**: 21 identical,
three equal to 1.8e-12 relative, which is `DOUBLE` summation order (two Burrmill runs differ from
each other by 7e-13). Default 512 MB, no refusal, 4 to 97 ms a statement on either engine. Both
stores opened in place from their older versions. Not rolled: `helsinki/scripts/migrate-last-two.sh`
in muster does it, asking the same statements before and after and undoing itself on a difference;
it is Chief's to run.

**Owed.** The roll of those two. The glibc floor (2.34) in the README was measured on the
4.0 artifact and wants re-measuring on the release build. Release notes for 4.1. Day pruning
through a one-row join side. muster.

---

## Four nests served by Burrmill; what each one took — 2026-10-01

Chief, during the day: the live subgraphs are the judge, not DuckDB; no month of checking; every
Lodestar nest on the latest build and on Burrmill before DuckDB leaves the binary.

| nest | served by Burrmill since (UTC) | build | settings |
|---|---|---|---|
| DIPS | 11:09 | 4.0.0-checked.1 | checked, defaults |
| GNS | 15:08 (11:11 to 11:37 first) | 4.0.0-checked.1 | Burrmill alone, defaults |
| allocations | 15:44 | 4.0.0-checked.2 | 2 permits x 2 GB, 8 threads, 6 GiB wall |
| QoS (ThinkPad) | 18:46 | 4.0.0-checked.3 | 2 permits x 2 GB, 8 threads, 6 GiB wall |

Still on DuckDB: the legacy archive (3.8.4, `serve`) and `data-services-nest` (3.12.1).

**GNS** came off Burrmill for 3.5 h over five `Rows` records, all one kittiwake query taking the
newest subgraph per deployment by `created_at` alone; the one checked was two subgraphs at
1715090775, each engine taking a different one. Fixed in the query (kittiwake#193), not classified:
the checker cannot see `created_at`.

**Allocations.** Every replay before 15:00 ran `gan-portable`, whose views were rewritten on
2026-09-24 on a branch never pushed. Production's views (`ASOF`, `LATERAL`, list lambdas) do not
define on Burrmill: six views absent. The seven commits were rebased onto the nest's main
(graph-allocations-nest#28), each rewrite compared whole against its original on DuckDB (23/23,
`lodestar_indexer_ledger` 1,678,525 rows), and put on the nest before the engine changed.
`scripts/lodestar-parity.sh` at pinned block 510,658,602 prints the same on DuckDB with the
originals, DuckDB with the rewrites and Burrmill with the rewrites: rewards 203/203 epochs, seven
fee epochs the nest books to the neighbouring epoch (each group sums to the subgraph's, to the wei),
nine known self-collections. The nest ran 4 permits x 256 MB, not the 2 x 512 MB muster records; at
1 GB Burrmill refuses 12-13 of the dashboard's statements and at 768 MB 18, so the limit stays at
2 GB and the permits went to two. Sweep through `/sql` on the copy (`sql_sweep_over_a_nest`,
whole views): DuckDB 4 x 256 MB 29/112/123 qps at 1/4/8 clients, p99 128-184 ms; Burrmill
1 x 2 GB 12/14/15 qps with 87 and 311 busy; 2 x 2 GB 11/26/28, 19 and 116 busy; 4 x 2 GB
11/49/49, 0 and 31 busy; p99 about 620 ms. About 2.5x DuckDB's time a statement there.
Three attempts: 14.5 min down on the first (a later drop-in put the permits back to four and the
gate refused; the script waited its full fifteen minutes on a nest failing in fifteen seconds), a
minute on the second (the roll worked and the script undid it: `journalctl | grep -q` under
`pipefail`), none wasted on the third.

**QoS** was replayed before it was touched and 5 of 23 views answered. Seven faults, in order:
`unhex`; `DATE + integer` past 2262 (the days became an interval of nanoseconds); `TRY` covering
only its outermost cast; every hash join building on the first table named, the 74-million-row one
(`BuildOnSmaller`: killed at 60 GB, then 30 s with 3-4 MB of build); `unnest` tainting every column
for the checked rule; `QUALIFY k = min(k) OVER (PARTITION BY ...)` sorting the whole input
(`PartitionExtreme`: 600 s, then 40 s); `count(DISTINCT (a, b, c))` beside another distinct kept as
`ScalarValue` structs (`DistinctRows`: past 500 s for a day, then 4 s). Lodestar's eight statements,
one day each, bench at 32 threads and no bound: DuckDB 0.6-0.7 s, Burrmill 3.2-4.1 s, equal to
3e-15 relative. Through `query_guarded` at 30 s: 7 of 8 at 512 MB (the indexer-daily one refused in
a sort merge), 7 of 8 at 2 threads and 2 GB (over the guard), 8 of 8 at 8 threads and 1 or 2 GB at
about 4.5 s. Whole views still fail at 512 MB (spill past 2 GB, or 600 s). Burrmill reads every
segment for a one-day statement where DuckDB reads that day's: the range in the join filter is not
pushed to the scan. Owed.

dialect-parity 269/269 after all of it; the allocations views unchanged by each rule.

**Owed.** Day pruning through a one-row join side. The shadow's `FloatOrder` rounds each side to
twelve digits and calls a pair straddling a rounding boundary `Rows`
(747.756973491 against 747.756973490); compare relatively. nest#24 (rewards denylist) is merged
and not live: it needs a re-index. The seven fee epochs against the subgraph, a views matter.
Removal: with Burrmill the default engine, 2,112 passed and 8 failed (`typeof`; an entity
relation loaded as text; an empty answer without column names; three tests of DuckDB's own).

---

## Checked mode, the stack on main, and what the allocations nest needs — 2026-10-01

Chief, the same day: migrate now and troubleshoot after, Lodestar being the only consumer; DuckDB
stays as the checker for about a month. So the shadow is turned round. `NUTHATCH_ENGINE=checked`
(nuthatch#1615) serves Burrmill and runs DuckDB behind it; a record still reads DuckDB first, so the
kinds mean what they meant. The stack is on nuthatch main as #1613 (`ee84d3c`), with #1612 for the
racing test. Review found the cold exposure and velocity folds counting a transfer past 38 digits
that the hot replay drops, as main's `COUNT(*)` always had; both now filter it.

**The allocations copy, served by Burrmill** (ThinkPad, `gan-portable`, 22 views and 81 dashboard
statements through `query_guarded`, checked mode). No `Rows` record in any of 16 replays. What
differs is memory refusals, and they follow the thread count, not the limit:

| limit | analytics threads | runs | runs with a refusal | refusals per run |
|---:|---:|---:|---:|---|
| 512 MB | 2 | 1 | 1 | 22, and one statement that spilled to the 600 s guard instead |
| 2 GB | 2 | 4 | 4 | 6, 1, 2, 1 |
| 2.5 GB | 2 | 1 | 1 | 1 |
| 3 GB | 2 | 4 | 3 | 1, 2, 0, 1 |
| 2 GB | 8 | 3 | 0 | |
| 3 GB | 8 | 3 | 0 | |

At two threads the refused statement changes from run to run (`lodestar_network`,
`lodestar_indexers`, `lodestar_delegator_stakes`, `lodestar_indexer_daily`, `lodestar_indexer_pool`),
one at 2.5 GB with the process at 1,098 MB resident: a `RepartitionExec` holding 657 MB, spillable
and never asked to spill under the greedy pool, beside a `HashJoinInput` that cannot. Why eight
partitions do not do this is not established. The fair pool is not the way out (27 and 29 September:
more refusals, and the lost wakeup). So the allocations nest takes 2 GB **and**
`NUTHATCH_ANALYTICS_THREADS=8`; 2 GB alone, the figure decided on 2026-09-29, refuses on every run at
nuthatch's default of two. The 27 September "2 at 2 GB" was at 8 threads without spill and is not
comparable with these.

The tally writes at 1, 2, 4, … and every thousandth, so its last line is a floor: 64 after a
103-statement replay.

DIPS: `nuthatch 4.0.0-checked.1` built on the ThinkPad from main `4722f56` plus #1615 (sha256
`caac5e12…`); the roll and its three roll-backs are muster `helsinki/scripts/roll-dips-checked*.sh`.
Not rolled yet. No rehearsal on a DIPS copy: there is none on the ThinkPad.

---

## The nuthatch stack merged with main; DIPS's shadow log says nothing either way — 2026-10-01

**The merge.** `pete/burrmill-watchdog` was 34 commits ahead of nuthatch `main` and 167 behind, with
sixteen of main's in the same entity files. Merged as nuthatch `34fc981`: six conflict hunks in four
files, one of them both sides fixing the dead circuit at the tip. Three things the text of the merge
did not show:

- Main's `entities::typed_for_check` (#1587) parsed through `json_serialize_sql` and
  `json_deserialize_sql` on a fresh DuckDB connection, new parser-role surface since the inventory at
  `711ae88`. Now sqlparser (`visit_relations_mut` drops the qualifiers); no production DuckDB in
  `entities.rs` again.
- Main gave the spill cap a 2 GB default (`cd03e81`); Burrmill's budget still defaulted to 100 GB.
  It takes `spill_cap_bytes` now, one cap for both engines.
- `Session::collect` returns `Collected` (#1609); six call sites followed.

On the ThinkPad, default features and `shadow-burrmill,graph`: integration 394/394 and 397/397,
network contract 21/21, library 1,512 and 1,658 passed with one failure on each,
`readers_racing_folds_never_answer_short_and_leave_no_file_behind`. Not the merge's: untouched
`origin/main` and the untouched branch fail it too, 2 runs of 2 each, and it passes 5 of 5 alone. It
asserts the readers got more than 120 answers in while 120 seals ran, which a 32-core box running
the whole library does not grant. Owed on main, apart from this stack. Clippy `-D warnings` and fmt
clean on both feature sets.

**The DIPS shadow log.** After three days on `3.12.1-shadow.1` the file holds one line, the planted
`printf` refusal. The shadow wrote differences only, so that reads the same for a nest that agreed
on every statement and one that was never sent any; the journal could not say either (a grep for
`shadow` matched the binary's own name on every line, 1,654 of them). Gate 2's first leg cannot be
read off that file. The shadow now counts every statement (nuthatch `5ffed4f`): `agreed`,
`both_refused`, `both_truncated`, `differed`, `skipped`, as `kind: "Tally"` lines in the same file
at 1, 2, 4, 8, … statements and every thousandth, and in the log. The DIPS binary is also built
from `711ae88`, before every Burrmill fix since 2026-09-28, so the release cycle is counted from a
fresh shadow build off the merged stack, not from 2026-09-28.

---

## Where the network nest's planning time goes — 2026-10-01

The network contract took 116 s on Burrmill against 32 s on DuckDB, 3.5-4.5x on every heavy test
alike. Per query, not per row: the fixtures are tiny, and each query's plan is 5,000-6,000 logical
and 3,000-3,700 physical nodes (about 30 views composed, nearly all distinct: 3,998 distinct
subtrees in 4,444, so sharing would not shrink them). Timed per rule on the slowest test (25
queries, 31 s):

| stage | s | notes |
|---|---|---|
| logical optimizer | 9.1 | DataFusion's; three passes, the first the dearest |
| physical optimizer | 8.4 | `EnsureRequirements` 5.2 of it |
| analyzer | 7.8 → 6.4 | `type_coercion` 2.6 (run twice); Burrmill's rules the rest |
| parse, view definitions | 5.5 | nuthatch defines ~33 views per query session |
| `information_schema` | 0.8 → 0 | rebuilt at every registration |

Fixed in Burrmill (`06d88f4`, `1001297`): `DuckComparisons` copied and compared every expression node
with itself (quadratic in depth) and re-derived every node's schema whenever it changed anything,
1.31 s to 0.41 s; the subquery and shift rules formatted every expression's name to find nothing,
0.65 s to 0.13 s; `information_schema` is built on first read.

Not fixed here:

- **`EnsureRequirements`** gives every child of every node a fresh statistics cache, so each walks
  its whole subtree: quadratic in plan size, 200 ms a query at this size. Fixed upstream
  (apache/datafusion#25098, merged 2026-09-17), not in 55.1.0; first in 56, due by the cadence
  late October or early November. Taken with that upgrade rather than a vendored fork.
- **`max_passes`**: one pass slowed execution (`lodestar_deployments` 143 to 533 ms); two matched
  three on all 22 views but saved only 1.9 s of 31. Left at DataFusion's three.
- What remains is DataFusion planning 5,000-node plans, about 1 s a query here against DuckDB's
  ~0.3 s end to end. On production data the scans dominate; for the network endpoint's latency
  it is the next thing to measure on a real nest.

## The network nest on Burrmill: contract 21/21, serve 97/97 — 2026-09-30

The last failures of nuthatch's network tests with Burrmill the engine, each a difference from
DuckDB:

- **A to-one lookup.** `(SELECT struct_pack(…) FROM indexer r WHERE r.id = c.indexer)`, which is
  how nuthatch's GraphQL compiles a relation: DuckDB runs a correlated scalar subquery that is not
  aggregated, failing only when an outer row finds two rows; DataFusion 55 refuses to plan it.
  `SingleRowSubqueries` makes it `{n: count(*), v: first_value(v)}` per correlation and checks `n`
  in the outer row, not in the subquery: decorrelated, the aggregate runs for every key of the
  inner relation, and the first version refused a duplicate key no outer row asked for.
- **The same lookup inside a paged list.** The rewrite that ranks a correlated `ORDER BY … LIMIT n
  OFFSET m` gave up on any select item holding a subquery. A subquery naming only the list's own
  relation and its own tables is now that relation's.
- **`>>` and `<<`.** The checked rule refused both. DuckDB's `>>` is 0 for a count outside
  `[0, bits)`, where Arrow's takes the count modulo the width (`1000 >> 64` was 1000); its `<<`
  refuses a bad count, a negative signed value and any bit shifted out; and an integer literal
  takes the other side's type, so `CAST(1 AS INTEGER) << 31` overflows. `CheckedShifts` rewrites
  both before coercion, which would otherwise have widened the literal.
- **A 78-digit cast** read "Cannot cast … to Decimal128(38, 10)": Arrow names its default decimal
  type whatever the target. The target was DECIMAL(38,0), per the decision to stay at 38 digits;
  the message now says so. Three nuthatch fixtures computed past 38 digits (2^256-1 balances,
  10^40 deltas); on Chief's call they keep exact arithmetic at the 38-digit line instead.

With the NULL subquery below: network contract 21/21 and `serve::tests` 97/97 on Burrmill, and the
contract still 21/21 on DuckDB. dialect-parity 260/260. The contract takes 116 s on Burrmill against
32 s on DuckDB; not yet looked at.

## An empty scalar subquery defaulted with coalesce came back NULL — 2026-09-30

`graph_network."isPaused"` read NULL on Burrmill where the fixture expects `false`. The view says
`coalesce((SELECT "isPaused" FROM controller_state), false)`, and `controller_state` is empty until
the Controller emits anything. DataFusion 55 types a scalar subquery by its column
(`expr_schema.rs:377`), so over a non-nullable column the `coalesce` is typed non-nullable, the
simplifier drops the default, and the NULL the empty subquery produces goes out, or, without the
`coalesce`, fails the batch as a NULL in a non-nullable column.

`NullableSubqueries`, first among the analyzer rules, wraps each such subquery in `maybe_null`, an
identity typed nullable, and restores the output names a wrapped aggregate or window changed. The
fuzzer's `stricter` count went from 7 to 0: those seven were this, refused with the non-nullable
error. `network_clock` passes; dialect-parity 251/251.

## A dead entity circuit went unnoticed at the tip — 2026-09-30

With Burrmill the default, two `e2e_entity_reorg` tests timed out in 7 of 11 full runs (a dead
entity circuit must end the nest, and must push an alert), never alone and never on DuckDB (0 of 9).
Not Burrmill's fault: nuthatch's. `apply_window` hands a window to the entity's circuit thread
without waiting, and `ensure_views_healthy` runs straight after; when the circuit dies a moment later
the check passes, and the next check was only after the next window. At the tip there is none, so the
nest carried on with a dead entity, the freeze those tests exist to catch. Burrmill's timing lost the
race more often. Caught with the suite's own log: both circuits died within a millisecond of each
other, one nest was quarantined, the other never noticed.

Fixed in nuthatch: the solo loop checks before each idle poll, and the runtime cursor quarantines,
through its supervisor, any live nest whose circuit died, before both of its waits. Ten full Burrmill
runs, none failing, against 7 of 11 before. No deterministic test: making a circuit die late on
purpose would take a hook into the thread; the two tests are the reproduction.

---

## A drifted table read from its first segment's columns, and a missing counter read as 0 — 2026-09-30

**`register_facts` took a table's columns from `files[0]`.** A column the ABI gained is in the later
segments only, so whenever an older segment came first the column read as NULL on every row, a
wrong answer with nothing to show for it; Burrmill answered the drifted fixture correctly by the luck
of its file order. The table's columns are now every segment's by name, DuckDB's `union_by_name`,
from the footers the definition already reads (kept per engine with each segment); two segments
that disagree on a column's type refuse, naming the table. `df_host`
`a_column_only_a_later_segment_carries_is_read_in_either_order`, which failed in one order (`count`
of the new column 0 against 1). With it nuthatch's Trino contract agrees on both engines, `sender_kinds`
1,000 and 500 on each.

**A hot row missing a counter read as 0.** `block_number`, `log_index`, `_seq` and `block_timestamp`
were non-nullable, so a row without one got 0: `min(block_number)` of a table with an unstamped hot
row was 0. Now NULL, as DuckDB's `read_json` reads it (`a_hot_row_missing_a_counter_reads_null`).

Suite 17/17, `dialect-parity` 241/241.

---

## Two decisions, and the checked rule refusing only what did not fit — 2026-09-29

Chief decided both open questions. **Allocations memory:** raise the figure (about 2 GB of
`analytics.memory_limit`, the nest's MemoryHigh above nuthatch's RFC-0047 2 GiB split), no spilling work.
**Aggregates over an overflowed `_dec`:** refuse, as Burrmill does, rather than answer from the
values that fit, as DuckDB does.

The second was asked as "when a value did not fit", and that was half of what Burrmill did. `SUM`
and `AVG` of a `TRY_CAST` were already exact and refused only on data; `MAX`, `MIN`, `count` and a
`SUM` of any expression over one were refused as plans, whatever the data. Now each tainted input
to such an aggregate is guarded, `CASE WHEN c IS NULL AND source IS NOT NULL THEN error(...) ELSE c
END`, its source exposed from the plan as the exact sums already do, so the statement refuses at a
row whose cast dropped a value and answers otherwise. Untraceable sources, `FILTER` over a tainted
value and window aggregates still refuse as plans. `df_checked` 14/14 with the old refusal test
rewritten both ways; suite 17/17; `dialect-parity` 241/241; `fuzz` 2,000 cases, 0 differing, 0
looser; views 22/22.

In nuthatch it takes the off-chain entity sum and the eval harness's `MAX`/`MIN` from refused to
passing: their data fits. The bigint test and the Trino fixture's `wide_values` aggregated over rows
built not to fit; both now say "the ones that fit" (`FILTER (WHERE NOT value_overflow)`, `CASE WHEN
NOT value_overflow THEN value_dec END`), the same answer on DuckDB, and the idiom the release note
gives authors.

**Corrected 2026-09-30: the wrong answer was Burrmill's, not DuckDB's.** This paragraph first
said nuthatch's DuckDB path miscounted the Trino contract's `sender_kinds`. It did not. On the DuckDB
default both sides were right and agreed; on the Burrmill default the translator took the table's
columns from Burrmill, and `register_facts` took them from the first segment only. The drifted
fixture's first segment has no `sender`, so the translation read it as absent and counted 1,500 with
none. DuckDB 1.5.4 answers 1,000 and 500 at every step (probed: `read_parquet`, `CREATE TABLE AS`,
`CREATE TABLE ... REPLACE`). The fix and its test are in the next entry.

---

## Nuthatch's suite on Burrmill: 164 failures to 6 — 2026-09-29

The ~1,400 nuthatch tests that query through `analytics` run on whatever `engine()` returns, which
is DuckDB; only the folds (about 240) ran on both. Flipping the default for a whole
`cargo test --no-fail-fast` (a scratch-only probe) is the honest count of what cutover would break:
164 on the first run, 6 after six fixes, four in Burrmill and two in nuthatch. The clusters, the
remaining six and the classification of the 69 tests that touch DuckDB directly are in
`docs/research/replacing-duckdb/test-inventory.md`.

Two of the fixes were wrong answers, not refusals: a historical window dropped unstamped rows where
DuckDB refuses, and a segment that was not Parquet went unnoticed until a query read it, so the
table failed outright instead of coming back reduced and flagged. A third was a security gap in
the tests: the `/sql` allowlist's cases (`read_xlsx`, `postgres_scan`, paths in table position) had
never run against Burrmill's walk. They do now, and pass.

---

## The watchdog could not stop Burrmill, and the hang is a lost wakeup — 2026-09-29

**The watchdog.** The cutover build has no DuckDB in front, so nuthatch's `/sql` guard must stop
Burrmill itself. nuthatch `pete/burrmill-watchdog`, `guarded_query_times_out_on_a_runaway_on_burrmill`:
the existing 250 ms guard, on a recursive CTE and on a nested-loop join of two `range`s. DuckDB
stops at the budget; Burrmill was still running at 120 s. `df_host`'s two new tests reproduce it
without nuthatch (`a_cancel_stops_a_recursive_cte`, `a_cancel_stops_a_join_of_ranges`, both past 60 s
before, both 0.16 s after).

The token was read in two places: `CancelExec` over nest scans, and between output batches. Neither
plan has a nest scan. The recursive CTE's work runs on spawned tasks while the caller waits, so the
caller now races the token (a 2 ms poll, as the gate's notice) and returns on it; dropping the
stream aborts the tasks. The join is worse: `range(1000000)` is one batch in one partition, nothing
is spawned, and `NestedLoopJoinExec` evaluates its filter over a million pairs per left row, 10¹²
in all, inside the caller's one poll, emitting nothing. No race outside that poll can win. So a
physical rule, `Cancellable`, last, wraps every join's output in `CancelExec` and every
nested-loop and hash join filter in an expression that fails once the token is set: the check is
where the join spends its time. `datafusion` suite green (17 binaries); nuthatch's guard test
passes on both engines against it (0.5 s). Not re-measured: the cost
of an atomic load per filter evaluation and per join batch on the views.

This matters beyond the cutover. Plain `/sql` is not admitted by `parquet_scans` (that is
`/q/{name}` only), so any client could send such a join, and the request's thread, and its permit,
stayed with it. On the DIPS shadow build, a statement DuckDB answers inside 15 s and Burrmill cannot
finish would have held DIPS's one permit; none is known, the path exists.

**The range join, the same day.** `RangeJoinExec::probe` built every index pair of a right batch
before chunking, and nothing it held was in the pool. Now the probe resumes from a cursor, at most a
batch of matches or 65,536 candidates a call, reading the token between calls (it rides in the
session config); a batch leaves as it is made; the collected side is reserved as it arrives. A
cancelled wide range join (4·10¹⁰ pairs) stopped after 14.6 s, now at once. It also found that
`count(*)` over any range join failed: no column projected, and a batch of none needs its row count
(`a_range_join_counts_with_no_column_projected`). Views 22/22 identical on both builds;
`lodestar_epochs`, its four range joins, 676-679 ms against main's 682-726 in alternating runs.

The honest pool costs one view at 512 MB. The replay (greedy, two threads, spill) refuses
`lodestar_epochs` where this afternoon's answered: 23 refusals against 22. The range join's build is
not among the top five consumers; a sort merge at 236 MB and hash-join builds at 121 and 129 MB,
none spillable, are. It was answering on memory the bound did not see.

**The hang, reproduced with symbols.** Burrmill main plus a fair-pool probe knob, nuthatch
`pete/sql-sweep` unstripped, the replay at 512 MB, two threads, spill on (ThinkPad, 17:50 EEST):

- greedy pool, production's configuration and the row the 27th never ran: **completed, 218 s**;
- fair pool: **parked** on `SELECT * FROM "lodestar_delegators"`, straight after
  `lodestar_delegator_stakes` was refused for memory in `FinalHashAggregateStream`, at 953 MB.

Stacks (`~/scratch/hang-fair-stacks-keep.txt`): the statement thread in `Runtime::block_on` inside
`sql_for_each`, both tokio workers idle (epoll, condvar), both fold workers asleep. Nothing runs;
some future the plan awaits is never woken. That is why the guard's cancel never landed on the
27th: the token is read only when something polls. The caller-side race above fixes that reach.
The lost wakeup is its own bug, and it is chance, not state: repeated, the pair
`lodestar_delegator_stakes, lodestar_delegators` in a fresh engine answered 20 times and parked on
the 21st, on `lodestar_delegator_stakes` itself, before any refusal. Every operator in
`lodestar_delegators`'s plan is stock DataFusion (sorts, repartitions, a partitioned hash join,
aggregates), so the suspect is a spilling operator under `FairSpillPool`, upstream. Nuthatch runs
the greedy pool. Four full fair-pool replays alongside the pair loop, one machine: three parked
(twice on `lodestar_delegator_stakes`, once after `lodestar_disputes`), one completed. Not one view,
and far likelier with the machine shared.

A first attempt at the rerun built DuckDB with debug info, which drops `NDEBUG`, and both runs died
on `D_ASSERT(new_remaining_size != 0)` in `temporary_memory_manager.cpp:28` after
`lodestar_escrow_transactions`: DuckDB breaks its own invariant at 512 MB, silently in release.

---

## `/sql` at 32 clients on DIPS and GNS, a cancelled join, and the hang — 2026-09-29

**The sweep.** nuthatch `pete/sql-sweep`, `serve::tests::sql_sweep_over_a_nest`: real `/sql` requests
through the router, the two permits, the 250 ms admission wait and its 503, one engine alone per
process (a test-only process-wide override), every authored view read whole, 10 s a point, on
copies of the nests from 2026-09-28. The memo (#1186) answers a repeated statement before the
permit gate and never reaches an engine, so the first run measured a hash map at 27,000 qps;
`NUTHATCH_SQL_MEMO_BYTES=0` measures the engines. Transcript:
`docs/bench/sql-sweep-thinkpad-2026-09-29.txt`.

| nest, clients | DuckDB qps | Burrmill qps | DuckDB p99 | Burrmill p99 | 503s | RSS D / B |
|---|---:|---:|---:|---:|---:|---|
| DIPS, 1 | 189 | 198 | 8 ms | 7 ms | 0 | 66 / 93 MB |
| DIPS, 8 | 342 | 353 | 34 ms | 32 ms | 0 | |
| DIPS, 32 | 348 | 355 | 125 ms | 103 ms | 0 | 81 / 99 MB |
| GNS, 1 | refused | 182 | | 11 ms | 0 | – / 95 MB |
| GNS, 32 | refused | 357 | | 122 ms | 0 | – / 102 MB |

Both engines saturate the two permits from two clients on, are fair to within one query per
client at 32, and never reach the admission limit: these nests' views take milliseconds. Burrmill
is level or a little ahead for 16-20 MB more.

**GNS's `developer_activity_weekly` is broken in production, on DuckDB.** Its `date_trunc('week',
to_timestamp(...))` is `date_trunc` over a `TIMESTAMPTZ`, which needs ICU; the bundled DuckDB may not
load it (`file system operations are disabled`), so the `CREATE VIEW` fails and the view is
skipped without a log line. The live nest on Helsinki answers "Table with name
developer_activity_weekly does not exist" (checked 2026-09-29). No consumer reads it (Lodestar,
kittiwake, muster). Burrmill serves it as written; on DuckDB, `to_timestamp(published_at)::TIMESTAMP`
would. The no-ICU class of the shadow classifier, found live.

**A cancelled join returns its memory** (`df_host::a_cancelled_join_returns_its_memory`, with
`Engine::memory_reserved`). A grouped cross join cancelled at 64-80 MB ends `Cancelled`, and the pool
is back to zero 2-5 ms after the caller has its error, as the partition tasks unwind. But the
cancel lands late: the join buffers one side, and one batch of the other is the whole product, so
at 1 GB the statement held 655-756 MB when it returned (0.38 s), and at 16 GB 2.6 GB (1.5 s). The
bound is one operator's work, as `cancel_token` says; the pool, always set in nuthatch, is the
backstop. Still owed: the per-query timeout at the 30 s guard for the cutover build.

**The hang, still parked.** The replay logged below as "A hang" (512 MB, two threads, fair pool,
spilling; its stdout is `~/scratch/mem-512-t2-fair-spill.txt` on the ThinkPad) is still alive, 42
hours on, at 0% CPU and 1.19 GB. All 37 threads sleep on futexes: 31 tokio workers and one in epoll
idle, the two fold workers and the main thread in plain `std` waits, and the test thread with one
thread it spawned both waiting on one address (op `0x189`). Spill files are open. The binary was
stripped (`strip = true`) and has been rebuilt since, so it cannot be symbolised; stacks and wait
channels are in `~/scratch/hang-2967563.txt`. "With symbols" means rerunning that configuration
with `CARGO_PROFILE_RELEASE_STRIP=false` and line tables until it parks again.

---

## Shadow mode under DuckDB's memory limit: Burrmill does not fit — 2026-09-27

Jules on nuthatch#1527: the shadow opened a second engine with no memory bound. True, and worse
than the shadow: the DataFusion path had never been bounded at all (DataFusion's default pool is
unlimited, plus a 1 GiB footer cache). `Engine::open_empty_within(bytes)` now holds the pool and the
footer cache under one figure, with spilling off; nuthatch gives it `analytics.memory_limit`, the
512 MB DuckDB gets, and its cursor-budget check counts two engines when the shadow is installed
(the shipped split is exactly 2,048 MB, so a shadow build must run one permit or less memory).

Replayed on the thinkpad copy (22 views, 81 dashboard statements) with the bound in place, run h
against run g's zero unexplained. Refusals are statements DuckDB answered and Burrmill refused for
memory. Transcripts: `shadow-replay-thinkpad-2026-09-27h.txt`, `shadow-memory-2026-09-27-*.txt`.

| Burrmill limit | threads | pool | spill | refusals |
|---:|---:|---|---|---:|
| 512 MB | 8 | greedy | off | 24 |
| 512 MB | 8 | greedy | on | 19 |
| 512 MB | 2 | greedy | off | 26 |
| 512 MB | 8 | fair | on | 31 |
| 512 MB | 2 | fair | on | **hung** |
| 512 MB | 8 | greedy, probe 1,000 | off | 24 |
| 1,024 MB | 8 | greedy | off | 10 |
| 1,024 MB | 8 | greedy | on | 9 |
| 1,024 MB | 8 | greedy, probe 1,000 | off | 11 |
| 2,048 MB | 8 | greedy | off | 2 |

What is actually holding the memory, from the refusals and `EXPLAIN ANALYZE`
(`engine-analyze-delegators-2026-09-27.txt`):

- **Operators that cannot spill.** `lodestar_allocations` alone in a fresh 512 MB engine: eight
  partitions each hold a hash-join build of ~26 MB and a final aggregate of ~26 MB, every one
  `can spill: false`. DataFusion's hash join does not spill at all. Not a leak: every refused view
  is refused alone too.
- **Partial aggregation that reduces nothing.** On `lodestar_delegators` the partial aggregates
  reduce 504.9k rows to 483.8k groups and are never skipped, because DataFusion probes only after
  100,000 rows per partition and each partition sees about 60,000. A probe at 1,000 skips them and
  takes the process peak from 3,910 to 2,488 MB, but changes no refusal: the pool's own accounting
  is not what the RSS is.
- **A 6.1 GB reservation.** `lodestar_delegator_stakes` and `lodestar_delegators` are refused
  asking for 6.1 GB in one allocation, in a process that peaks at 2.5 GB. The `array_agg` of
  structs feeding the rewritten `list_reduce` fold is the suspect: its accumulators hold
  `ScalarValue` structs sliced from larger batches. Not yet proven.
- **A hang.** Two threads, the fair pool and spilling together parked every worker on
  `lodestar_delegator_stakes` for 2 h 42 m at 0.2% CPU. The cancel token cannot reach a statement
  whose futures are never polled. Not reproduced or traced; the binary had no symbols.

So gate 2's "Burrmill alone under the nest's `MemoryHigh`" is not met at DuckDB's figure, and no
setting reaches it: DuckDB answers these views in 512 MB by spilling, and DataFusion's join cannot.

### The 6.1 GB, found and fixed

It was the ordered `array_agg`. Run alone, the `folded` CTE of `lodestar_delegator_stakes` asks
for 6.0 GB even in an 8 GB pool, while its `exact` sibling (all the checked sums) answers in 512 MB.
DataFusion 55 has no groups accumulator for `array_agg(x ORDER BY k)`: every one of 484k groups gets
a row accumulator holding `ScalarValue` structs, each a one-row slice whose size counts the array
behind it. `df/ordered_agg.rs` is one: values stay in their arriving batches, entries are
`(group, batch, row)`, keys are row-encoded, a group is sorted once at emit, the state is the
built-in's. The fold now answers at 512 MB; `engine-views` 22/22 identical, `dialect-parity`
210/210, `fuzz` 2,000 with no difference, and a test drives the partial-skip path (193.5k rows
skipped) as well as the merge.

Refusals barely move (24 at 512 MB; 12, 10 and 8 on three identical runs at 1 GB), because what is
left is the other class: `Memory Exhausted while SpillPool` from the repartition buffers, and the
joins and final aggregates that cannot spill. The three 1 GB runs are also a finding: near the
bound, which statements are refused depends on which partition reserves first.

### What each view needs, 2026-09-28

The refusal count swings with scheduling, so the measure is the smallest budget at which each view
answers twice running (`burrmill-bench engine-analyze`, thinkpad copy). DuckDB answers every one at
512 MB. Twelve views need 128-384 MB. The rest are one family, all reading
`lodestar_indexer_ledger`:

| view | 8 threads | 1 thread | 8 threads, spilling |
|---|---:|---:|---:|
| `lodestar_indexer_ledger` | 1,536 | 1,536 | 1,536 |
| `lodestar_indexers` | 2,048 | 1,536 | 1,536 |
| `lodestar_network` | 2,048 | 2,048 | 1,536 |
| `lodestar_indexer_daily` | 2,048 | - | 2,048 |
| `lodestar_indexer_deployment_daily` | 2,048 | 3,072 | 2,048 |
| `lodestar_allocations` | 768 | 1,024 | 512 |
| `lodestar_epochs` | 768 | 768 | 768 |

Neither lever closes it. One thread plans no repartition at all and the ledger still needs 1.5 GB;
spilling moves three views one step. At one thread and 1 GB the ledger's holders are two hash-join
builds of ~108 MB (the key-carrying `ASOF` replacement joining back to `cuts` and
`pool_shares_series`), two sort merges of 101 and 76 MB, and a sort-preserving merge of 84 MB, none
spillable. Carrying values instead of keys (`LAST_VALUE(... IGNORE NULLS)`) removes the joins but
still fails at 768 MB in the sort, and ran 37 s against DuckDB's 0.75. Not the window function: an
earlier line here blamed a re-scan of the unbounded frame, and measuring it alone on
`rewards__rewards_assigned` says otherwise (Burrmill 416 ms dense, 116 sparse, 421 on `HUGEINT`,
against DuckDB's 36-44; `max` 50). What else in that rewrite costs 37 s is not found; it was dropped.
The in-argument spelling DuckDB uses, `last_value(x IGNORE NULLS)`, which graph-network-nest's views
use, is now read (`dialect-parity` 212/212).

### DIPS and GNS fit, 2026-09-28

Their sealed segments copied from Helsinki (DIPS 308 KB, GNS 18 MB; the live store left alone):
`engine-views` 2/2 and 7/7 identical to DuckDB, and every view answers at 32 MB, two GNS views at
64, against DuckDB's 512 and the units' `MemoryHigh` of 1 GB and 1.5 GB. The ledger family is the
allocations nest's alone. Not covered: the unsealed tip and Lodestar's own statements to these
nests, which is what a live shadow is for.

### Owed

- A decision on the ledger family: the DataFusion path holds every sort and join build of the plan
  at once and recomputes the whole history per request; DuckDB streams it under 512 MB.
- A decision on the budget: a larger Burrmill figure per permit means amending nuthatch's RFC-0047 2 GiB
  split; or engine work on the non-spilling operators.
- The hang, with symbols.

---

## Shadow mode's first day — 2026-09-27

Phase 2b in nuthatch (`pete/shadow-session`): `ShadowEngine` pairs a DuckDB and a Burrmill session
per nest, forwards every catalogue call to both, serves DuckDB's rows and compares Burrmill's as a
multiset. Burrmill's session is a `burrmill::Engine` opened empty, with tables registered as the
policy code binds them: the new `Engine::register_facts` takes the same segment list, declared
columns, hot rows and window DuckDB is given, and `register_rows` the label snapshots. Rows cross as
nuthatch JSON via `df::encode`, never as arrow, because the two arrows differ (58 and 59).

Replayed on the thinkpad's 1,925-segment copy through the production path (`query_guarded`), every
authored view whole and then the dashboard's 81 statements generated from kittiwake's own SQL
functions with ids resolved from the nest (`docs/bench/shadow-replay-thinkpad-2026-09-27{a,b,c,d}.txt`,
`docs/bench/dashboard-statements-2026-09-27.sql`):

- **a.** 5 differences in 22 views, all one fault of the harness: DuckDB's `collect` truncates at a
  64 MiB byte cap, the Burrmill session applied only the row cap. The cap is now `collect`'s
  contract, and once both engines have truncated nothing sound is left to compare.
- **b.** 22 views, 0 differences.
- **c.** Views clean; of 65 statements the nest can answer, 6 differed. Four were one Burrmill bug:
  an aggregate aliased to its own source column and repeated in `ORDER BY`
  (`... CAST(SUM(CAST(tokens AS HUGEINT)) AS VARCHAR) AS tokens ... ORDER BY SUM(CAST(tokens AS
  HUGEINT))`) planned the `ORDER BY` against the alias and refused a duplicate field. A name inside
  an `ORDER BY` expression is now the source column, as DuckDB and Postgres read it; only a bare
  `ORDER BY x` means the alias. `dialect-parity` 210/210. Two were the daily-flow charts summing
  `CAST(tokens_dec AS DOUBLE)`, which the checked rule refuses because `_dec` is NULL past 38 digits
  and the sum would drop those rows silently; the statement now casts `tokens` itself (kittiwake
  `pete/dump-nest-sql`). Three more were `SELECT id FROM x LIMIT 1` picking different rows, which
  is the engine's choice: recorded as `Unordered`, not counted.
- **d.** With both fixes: 22 views and 65 statements, one difference left, a `DOUBLE` sum differing
  in its sixteenth digit (`3428611.8956044842` against `…847`), summation order. Recorded as
  `FloatOrder` from now on: the same rows once every float is read to twelve significant digits.
- **e.** 22 views and 65 statements: four records, three `Unordered` and one `FloatOrder`, **nothing
  unexplained** (`shadow-replay-thinkpad-2026-09-27e.txt`).
- **f.** The same with Burrmill's cancel token as the shadow's interrupt handle and RSS in each
  record: four records, nothing unexplained (`shadow-replay-thinkpad-2026-09-27f.txt`).
- **g.** The parser role in shadow too: the security walk moved behind `Session::reach` (DuckDB's
  AST walk unchanged), Burrmill's `inspect::reach` beside it, a walk that reaches less or admits
  what DuckDB refused counted as `ParserLooser`. Over the 22 views and 81 statements, every one
  walked by both: **no parser disagreement of either kind**, the same four records as before, and
  the JSON-lines sink written (`shadow-replay-thinkpad-2026-09-27g.txt`, `shadow-2026-09-27g.jsonl`).

The shadow runs inline and is skipped once the primary has used half the guard's budget, so a
shadowed request pays for both engines; Gate 2's p99 is measured with it on. Burrmill's session has
no cancellation handle yet, and the fold table map is fixed when an engine opens, so a host-registered
table's aggregates run as DataFusion plans them, to the same answer.

---

## The nest re-run, with everything since 25 September in — 2026-09-27

`engine-views` over `~/scratch/gan-portable` on the thinkpad (1,925 segments, the rewritten views),
`TZ=UTC`, at `nice 10` beside the hosted platform and the QoS nest, which is not how the 25th was
measured and is noted for that reason. **22/22 views identical, both zero-row checks agree, and
41,380 ms DuckDB against 26,194 ms Burrmill: 0.63x time-weighted, 22/22 within 1.5x.** The worst
is `lodestar_disputes` at 1.11x (10 ms against 9); the best `lodestar_deployments` at 0.14x (165
against 1,173). `lodestar_indexer_ledger`, the largest at 1.68 M rows, is 0.86x. The two pinned
checks still disagree with their August fixtures on both engines, as before. Transcript:
`docs/bench/engine-views-thinkpad-2026-09-27.txt`.

---

## List comprehensions, which the views still write — 2026-09-26

`lodestar_network`, `lodestar_params` and the indexer-deployment daily view fold a hex word with

```sql
list_reduce([CAST(strpos('0123456789abcdef', c) - 1 AS HUGEINT)
             FOR c IN string_split(substr(lower(CAST(result AS VARCHAR)), -32), '')],
            lambda acc, d: acc * 16 + d)
```

sqlparser stops at `FOR`. `list_transform` and `list_filter` already agree with DuckDB, so the
text is rewritten into those before either parser sees it: `[expr FOR x IN list]` is
`list_transform`, and `IF` is a `list_filter` in front of it. A list literal is left alone, and
so is the word `FOR` inside a string. `reach` uses the same rewrite, so the tables inside the
comprehension are not hidden by a parse error.

`string_split('ab', '')` folded that way is 171 on both. `dialect-parity` 209/209. Fuzz, 6,000
cases on three seeds: nothing stricter, nothing looser, no differences. The nest was not re-run.

**2026-09-27:** the scanner skipped strings and comments but not `"quoted identifiers"`, so a
column named `"for x"` read as the keyword; it skips them now, and a test says so.

---

## The fuzzer widened to the idioms the views write — 2026-09-26

The grammar had gone quiet. The census still counts things it never drew: `substr` from the end,
`string_split`, `xor`, `::` casts, list literals, `list_reduce`, `decode(from_hex)`, `FULL JOIN`,
`VALUES`, `power`, `least`, and `unnest` in the select list. `::`, the joins, `VALUES`, `power`,
`least` and `list_reduce` already agreed. The rest did not.

- **`substr` from the end was a wrong answer.** DataFusion's unicode planner binds `substr` itself
  and rejects a negative length, so `substr('10', -3, 2)` came back empty where DuckDB gives `1`.
  The call is renamed to `burrmill_substr`, including `SUBSTRING(s FROM n FOR m)`, which is not a
  function call. A negative start counts back from the end, a start short of the first character
  shortens a written length, and a negative length runs backwards.
- **`string_split`**, also `str_split` and `string_to_array`. An empty delimiter splits into
  characters, an empty string stays one empty piece, and a NULL delimiter is the string unchanged.
- **`xor`.** A literal takes its partner's integer type, and `BIGINT` beside `UBIGINT` is HUGEINT.
  Bitwise, so the checked rule allows it.
- **A list index had the wrong type.** `[5, block_number, 4]` was `DECIMAL(20,0)`, a digit string,
  where DuckDB fits the literal and the element stays `UBIGINT`. The list itself is left as
  DataFusion typed it, because a `list_reduce` lambda is already bound to that; the indexed value
  is cast. A negative literal beside an unsigned column is HUGEINT.

`FROM unnest(...)` is not planned. The select-list form, which is where the views write it, agrees.
The only table functions remain `range` and `generate_series`.

`dialect-parity` 206/206. Fuzz, 8,000 cases on four seeds: nothing stricter, nothing looser, no
differences. A cast of `TIMESTAMPTZ` to `DATE` joins the no-ICU class: nuthatch's DuckDB refuses
it, Burrmill computes it. The nest was not re-run.

---

## The fuzzer's refusals audited, and a DuckDB bug traced to its line — 2026-09-26

**Both-refuse was mostly the generator.** 36 to 76 cases a seed failed in both engines, mainly on
DuckDB binder errors the generator had written: HAVING comparing a text or timestamp aggregate
with a number, `DISTINCT ON` over `ev` reading `l`, a literal as a `string_agg` tie-break, and
unsigned casts of negative numbers. Fixed, it is 4 to 29 a seed, nearly all UBIGINT subtraction
overflowing in both. About 150 more cases a seed now test something.

**Then 25,000 cases on five fresh seeds** (`CASES=5000`), which found:

- **`UINTEGER // BIGINT` is BIGINT in DuckDB**, where Burrmill gave HUGEINT to every mixed-sign
  `//`. It is now their union: HUGEINT only past 64 bits (UBIGINT beside a signed type).
- **The DuckDB text/timestamp bug, root cause.** `ComparisonSimplificationRule` moves the cast in
  `CAST(ts AS VARCHAR) < 'x'` onto the constant; the cast of `'x'` to TIMESTAMPTZ yields NULL, the
  guard that would stop it skips NULL, and the comparison becomes `ts < NULL`: every row dropped,
  or under `IS NOT DISTINCT FROM`, the NULL timestamps kept. It happens once the session's zone has
  been consulted (`year()`, `date_trunc`, `current_setting`, `SET TimeZone`), and the switch is sticky
  for the connection, which nuthatch caches per nest. Written up in
  `docs/upstream/duckdb-cast-comparison-null-constant.md`, not filed.
- **The harnesses had been switching it on.** They ran `SET TimeZone = 'UTC'`, where nuthatch sets
  no zone. The bench now sets `TZ=UTC` in the process before any thread, as a UTC host would. With
  that, DuckDB also refuses `TIMESTAMPTZ + INTERVAL` again (no ICU), as nuthatch does; the fuzzer
  counts Burrmill computing it as designed.
- Out-of-range casts to DECIMAL join optimiser order (Burrmill refuses them too when evaluated).

The fuzzer also takes `SQL=<query>` now, to run one query on its fixture.

`dialect-parity` 194/194. Fuzz, 16,000 cases on eight seeds: nothing stricter, nothing looser, one
difference (the DuckDB bug, sticky from an earlier query in the run). The nest: 22/22, 0.655x;
`deployment_signal` 1.05 to 1.18x over five runs.

---

## The fuzzer widened again: the shapes views use — 2026-09-26

Six new query shapes: grouping over a derived table (with `GROUP BY ALL` and `ORDER BY ALL`), a
window over grouped output (`rank() OVER (ORDER BY sum(x))`), a CTE joined to itself, `QUALIFY` and
`DISTINCT ON`, lists and JSON inside aggregates, and the anti-join written as `LEFT JOIN ... WHERE
l.addr IS NULL` beside `IN` over an ordered, limited subquery. A list itself has no nuthatch JSON
encoding, so lists appear only inside expressions.

What it found:

- **`ORDER BY ALL`** was refused over anything but bare columns. It is now `ORDER BY 1, ..., n` with
  its direction and NULLS order on each, wherever the select list has no wildcard.
- **List accessors refused by the checked rule** (36 to 56 a seed): `array_element`,
  `array_length`, `cardinality`, `array_max`, `array_min` and `array_position` return an exact type
  but do no arithmetic, and are on its list now.
- **A wrong answer: `round(x / 7, 1) - CAST(1 AS HUGEINT)`** gave `-2` where DuckDB gives `-1.7`. At
  coercion `x / 7` was still integer division, so the subtraction was typed as decimal arithmetic and
  the round cast to DECIMAL(20,0); `/` became DOUBLE only after, and the fraction was cast away. `/`
  is now DOUBLE before coercion as well.
- **DataFusion refused its own plan** in `eliminate_group_by_constant`: a `TRY_CAST(k AS BIGINT)`
  key beside `k` (a cast is named after its operand in DataFusion) was recomputed in a projection
  where the two names collided. The rule, which only drops a key that is a function of another,
  is left out of Burrmill's optimizer.

`dialect-parity` 194/194. Fuzz, 10,000 cases on five seeds: no differences, nothing looser, one
stricter (`TIMESTAMP <> 16` over no rows). The nest: 22/22, 0.623x; the slowest view,
`deployment_signal`, at 1.12 to 1.36x across three runs.

---

## Aggregate subqueries correlated through a non-equality — 2026-09-26

`(SELECT count(*) FROM lbl x WHERE x.weight > e.amount)`: DataFusion 55 decorrelates a correlated
aggregate only through `inner column = <outer>`, and has no general decorrelation behind an option.
After the widened fuzzer, this was nearly all of what remained stricter (21 to 29 a seed).

`NonEquiCorrelation` (`df/correlate.rs`) does the textbook rewrite, only where DataFusion would
refuse: the outer rows numbered with `row_number()`, the subquery's relation (its other conjuncts
applied first) LEFT JOINed on the correlated conjuncts, grouped by the row number and the outer
columns, and the subquery's own projection evaluated over each group. `count(*)` counts a marker
only matched rows carry, so no match is 0 and `count(*) + 1` is 1. Only aggregates whose answer
over no rows is their answer over one NULL-extended row qualify (`count`, `sum`, `min`, `max`,
`avg`, `bool_and`, `bool_or`, `string_agg`, and the checked sums and average); `array_agg` would
collect the NULL, so it stays refused, as does a subquery whose relation shares a name with the
outer query's.

`dialect-parity` 189/189. Fuzz, 10,000 cases on five seeds: stricter **127 to 4**, the four being
`TIMESTAMP <> 16` over no rows (DataFusion refuses at plan time what DuckDB fails only on a row);
no differences, nothing looser. The nest: 22/22, 0.615x, the slowest view 1.30x.

---

## The fuzzer widened, and what it found — 2026-09-26

The grammar now draws what recent faults had in common: casts to other integer widths and inside
aggregates, numbers of mixed type (an integer beside a decimal literal or a DOUBLE), aggregates
with `FILTER`, arithmetic between aggregates, HAVING on any aggregate, two grouping keys, `IN`
lists holding a NULL, windows over an expression and without a partition, and correlated scalar
subqueries in the select list, some correlated through a non-equality. Two new outcome classes:
**FLOAT-ORDER**, rows equal but for floats within 1e-12 relative (a DOUBLE sum taken in another
order, which each engine's partitioning decides), and out-of-range cast errors joining
**OVERFLOW-ORDER** (DataFusion unwraps `CAST(x AS UBIGINT) > 20` to `x > 20`, so the cast that
DuckDB fails on is never evaluated; no value wraps, checked).

On first run: 1 to 5 different answers and 1 to 5 looser per seed. All are fixed but the classes above:

- **My own fault, from the exact `avg`.** It judged an argument an integer before coercion, where
  `CASE WHEN .. THEN -8 ELSE 2.5 END` or `round(x / 7, 1) - 28` still reports its first branch's
  or operand's integer type; the cast to DECIMAL(38,0) rounded 2.5 to 3. Now only for an argument
  that is an integer on its face (integer columns, literals, casts, `+ - * %` of those).
- **A float beside any other number in a set operation is DOUBLE**, as DuckDB has it. DataFusion
  made DECIMAL and cast the floats in, printing their binary error as exact digits
  (`1738368000.000000100663296`), in `UNION` and in `EXCEPT`/`INTERSECT` alike.
- **An integer literal beside a HUGEINT branch** of a `CASE` now makes the `CASE` HUGEINT before
  coercion, so a set operation over it gets DuckDB's type.
- **`burrmill_decimal_to_double` declares its nullability** (NULL exactly where its input is): a
  union's schema derived before it ran disagreed with the physical plan.
- **Binder-time refusals.** DuckDB refuses an integer literal beside a DATE or TIMESTAMP in an
  ordering comparison, `BETWEEN`, `greatest`, `least` and `coalesce`, whatever the rows; Burrmill
  made the integer a date. `=`, `<>` and `nullif` DuckDB binds as a cast that fails only on a row,
  so those are left, and DataFusion's plan-time refusal of `TIMESTAMP <> 16` is stricter only over
  no rows.
- **A subquery inside an aggregate in HAVING** (`HAVING count(*) FILTER (WHERE x IN (...))`) is
  counts, as in the select list; DataFusion's membership test cannot be planned there.

`dialect-parity` 184/184. Fuzz, 10,000 cases on five seeds: no differences, nothing looser; stricter
23 to 29 a seed, nearly all scalar aggregate subqueries correlated through a non-equality
(`x.weight > e.log_index`), which DataFusion 55 does not decorrelate. The nest: 22/22, 0.642x, the
slowest view 1.25x.

---

## DuckDB's constants moved, so its overflows are the ones that surface — 2026-09-26

`WHERE block_number - 95 > 0` over a UBIGINT: DuckDB answers, Burrmill refused on the underflow at
`1 - 95`. DuckDB's `MoveConstantsRule` rewrites it to `block_number > 95` before anything is
evaluated. It was `KNOWN` as optimiser territory; it is a rule, so it is now ported case for case
(`df/constants.rs`, after coercion and before the checked rule): `+`, `-`, `*` over integers
against an integer constant, in any expression; `c - x` and a negative multiplier flip the
comparison; a product not cleanly divisible decides `=` and `<>` outright; a moved constant that
does not fit decides `=` (false, or NULL) and otherwise leaves it; a NULL constant makes the
comparison NULL except under `IS [NOT] DISTINCT FROM`; applied until nothing moves.

DuckDB's binder makes `x BETWEEN a AND b` two comparisons first, so the constants move through it
too. That exposed a typing difference underneath: DuckDB fits integer literal bounds to the
operand, where DataFusion widened `UBIGINT BETWEEN 5 AND 9` to DECIMAL(20,0); the bounds are now
fitted where they fit.

What remains of the fuzzer's overflow-order class is `BETWEEN 9 AND 5`, which DuckDB's filter
combiner recognises as empty without evaluating the operand. `dialect-parity` 175/175, its only
`KNOWN` entry now DuckDB's own bug. Fuzz, 10,000 cases: overflow-order 3 to 1, stricter 0, the four
DuckDB-bug differences. The nest: 22/22, 0.648x, the slowest view 1.06x.

---

## Aggregates that differ only by a cast — 2026-09-26

DataFusion names an expression without its casts (deliberately, so `CAST(a AS INT)` is still `a`),
so `max(CAST(x AS UBIGINT))` beside `max(CAST(x AS BIGINT))` or `max(x)` in one SELECT gave the
Aggregate two fields of one name, and the SQL planner refused it before any rule ran, aliases or
not. It was a `KNOWN` refusal for `avg`; it held for every aggregate.

After the rewriter has finished with a query, each aggregate whose cast-stripped text repeats another's
gets `FILTER (WHERE n = n)`, a distinct `n` per variant: its name changes, its answer does not, and
identical aggregates keep one tag. Output names are DuckDB's, printed from the query as written.

`dialect-parity` 172/172; fuzz unchanged (stricter 0, four DuckDB-bug differences); the nest 22/22,
0.642x, the slowest view 1.25x.

---

## Correlated subqueries: the last stricter class — 2026-09-26

After the counts rewrite the fuzzer's stricter cases were all subqueries DataFusion 55 would not
decorrelate. Three causes, three fixes:

- **Correlated through an expression of the subquery's rows.** DataFusion pulls a correlated `=`
  over an aggregate only when the subquery's side is a column (`can_pullup_over_aggregation`), so
  `(SELECT count(*) FROM label l WHERE lower(l.addr) = lower(t.x))` stayed correlated and the
  physical planner refused it. `KeyedCorrelation` (`df/correlate.rs`) computes the subquery's side
  as a column beneath the filter, the shape DataFusion already handles. This was a `KNOWN` refusal
  in `dialect-parity`; it is now a case that passes.
- **Inside an aggregate, over a join.** DataFusion decorrelates a scalar subquery in a projection or
  a filter, never in an aggregate's arguments. The AST wrapper covered one relation only.
  `SubqueriesBelowAggregates` lifts each into a projection beneath the aggregate, where qualifiers
  survive however many relations there are, and the outputs keep their names.
- **My own rewrite's reach.** It visited only a query body that is a plain SELECT, so an `EXISTS`
  in a `UNION` or `EXCEPT ALL` branch was left for DataFusion to refuse; every branch is now
  visited. And a bare outer name over a join could not be qualified, so `"to" IN (SELECT "to" FROM
  ev ...)` over `ev e JOIN lbl l` was left alone. It is now qualified by the one relation whose
  catalog columns include it; a name two relations have, a derived table, or a table a CTE may
  shadow leaves it bare, and the rewrite declines rather than guesses.

`dialect-parity` 170/170. Fuzz, 10,000 cases on five seeds: stricter **33 to 0**; the four
differences are the DuckDB text/timestamp bug; nothing looser. The nest: 22/22, 0.643x, the
slowest view 1.26x.

`dialect-parity` also takes `SQL=<query>` now, to run one query against its fixture and print both
answers.

---

## JSON, `arg_max`, `range`, `TRY`, and an inexact `avg` — 2026-09-26

The functions nest views and ad-hoc queries reach for that DataFusion lacks or spells differently,
each measured against DuckDB with `duck-eval` (`df/duckfns.rs`):

- **JSON**: `json_extract`, `json_extract_string`, `json_type`, `->` and `->>`, over DuckDB's path
  forms (`$.a`, `."k"`, `[n]`, `[#-n]`, a bare key, an integer index). `json_type` of a
  non-negative integer is `UBIGINT`, as DuckDB has it. `from_json` with a structure literal builds
  the struct or list, and struct fields keep their DuckDB names in results (`s['a']` is `a`).
- **`arg_max`/`arg_min`/`max_by`/`min_by`**, `range` and `generate_series` as table functions
  (literal arguments, capped at 10M rows), `len`, and the unsigned type names (`UINTEGER`,
  `UBIGINT`, `UTINYINT` and the rest).
- **`TRY(expr)`** evaluates the batch, and on an error goes row by row with NULL for the rows that
  fail. `Engine::register_scalar_udf` lets a host add its own functions, `TRY` included.

**An inexact `avg`.** DataFusion coerces an integer `avg` argument to DOUBLE and sums in floats, so
`avg(CAST(x AS UBIGINT) * 2982776736)` over 63 values near 1e9 was off in the last digit. An integer
argument is now cast to `DECIMAL(38,0)` before coercion, and the checked rule makes the exact `avg`
with DuckDB's DOUBLE result. Arithmetic between a DOUBLE and an exact number, which that newly
produces (`avg(n) + 0.5`), is DOUBLE arithmetic as in DuckDB, not a refusal; a float compared with a
decimal compares as DOUBLE; and a `CASE` mixing signed and unsigned integers with a scale-0 decimal
unions to `HUGEINT`.

The DuckDB text/timestamp bug is wider than first written: any comparison of text with a timestamp
cast to text drops rows, with or without a date part projected (`KNOWN`; not reported upstream).

`dialect-parity` 165/165. Fuzz, 10,000 cases on five seeds: four differences, all that DuckDB bug,
nothing looser. The nest: 22/22, 0.604x, the slowest view 1.34x.

---

## Subqueries as predicates — and a DataFusion wrong answer — 2026-09-26

The fuzzer's largest stricter class was `EXISTS`/`IN (subquery)` outside a `WHERE`: DataFusion
decorrelates them only as filter predicates. It does decorrelate scalar aggregates anywhere, so
(`df/subqueries.rs`):

- **In a select list**, `EXISTS (q)` is `(SELECT count(*) ... q's WHERE) > 0`, and `x IN (SELECT y
  ...)` is three counts giving SQL's three values exactly as DuckDB does (no rows false, NULL `x`
  NULL, a match true, a NULL `y` NULL, else false). `x` moves into the subquery's `WHERE`, so it
  is rewritten only when every column in it is qualified by a name the subquery does not bind; a
  bare column is qualified first when the outer query reads one relation.
- **Inside an aggregate** (`bool_and(x IN (...))`, `sum(CASE WHEN x IN (...) ...)`), over one
  relation, the predicate is computed per row in a wrapper keeping the relation's name.

Measured on 5,000 cases: stricter **162 to 18**, all but one subqueries correlated through an
expression of the outer row, which DataFusion 55 cannot decorrelate anywhere (`KNOWN`).

**Found on the way, a DataFusion wrong answer**: `IN (subquery)` inside `OR`, `NOT` or `CASE` in a
filter goes through a mark join that loses SQL's NULL. `WHERE x = 3 OR x NOT IN (SELECT y FROM
(VALUES (1), (NULL)))` returned `x = 2`; the answer is NULL, so only 3. As a condition of its own it
is right (a null-aware anti join). There, Burrmill now keeps DataFusion's membership test and
supplies the NULL cases from counts over the subquery alone, so nothing moves between scopes;
a subquery that is not a plain SELECT is refused.

**Two faults of my own, both caught by the nest before they left the machine.** The first version
walked into nested queries and rewrote a `NOT IN` in a scalar subquery's `WHERE`, where the moved
operand, a bare `"subgraphID"`, rebound to the inner table: `lodestar_network`'s active subgraph
count read 0 against 16,154. The second correlated counts through `LOWER(...)`, which DataFusion
could not plan, and `lodestar_indexers` failed. Rewrites now stay at their own level, and the fuzz
grammar now draws bare outer names that the subquery also binds, `NOT IN` in a scalar subquery's
`WHERE`, and `IN` under `OR`. The first shape is also in `dialect-parity` (135/135).

Also: `//` beside a DOUBLE is ordinary DOUBLE division in DuckDB (`7.5 // 2` is 3.75);
`f(flag = 'x')`, which sqlparser reads as a named argument, is a comparison again;
`to_timestamp` of a number is built in microseconds, so dates past 2262 no longer overflow; and a
`CASE` DataFusion's logical planner proves non-null but its physical one cannot is declared
nullable, which had made DataFusion refuse its own plan wherever such a value was grouped twice.

Nuthatch's DuckDB, built without ICU, cannot add an INTERVAL to a TIMESTAMPTZ (a binder error, and
in one fuzzed form a NULL); Burrmill computes it. Fuzz, 13,000 cases on three seeds: no difference
but DuckDB's, nothing looser. The nest: 22/22, 0.638x, all within 1.5x.

---

## The fixed cost of a query — 2026-09-25

Timed on the thinkpad nest with temporary hooks (not committed), last of six runs:

| | `SELECT 1` | `count(*)`, small table | `lodestar_disputes` |
|---|---:|---:|---:|
| identifier names rebuilt | 0.43 ms | 0.50 | 0.49 |
| parse and rewrites | 0.22 | 0.23 | 0.22 |
| SQL to plan | 0.04 | 0.15 | 0.18 |
| analyzer and optimiser | 0.08 | 0.47 | 2.54 |
| physical planning | 0.05 | 0.16 | 0.90 |
| execution | 0.08 | 0.91 | 3.14 |

**The identifier names are now built once** and rebuilt only when a table or view is registered;
a statement's own aliases go in an overlay instead of a copy of thousands of names. That was half
of a trivial query's time, and half a millisecond off every one.

**The rest of a small query is DataFusion fanning out.** `lodestar_disputes` runs 18 operators,
four of them repartitions to 8 partitions, for 8 rows. Measured over the whole nest:

| | time-weighted | `disputes` | `epochs` | `port_queue` |
|---|---:|---:|---:|---:|
| as it is | 0.635x | 16 ms | 664 | 52 |
| round-robin repartition off | 0.648x | 8 | 784 | 71 |
| statistics collected | 0.643x | 15 | 723 | 53 |

Neither is a setting to ship: the small view halves and mid-sized ones pay more.

**Then, the adaptive version (`df/smallinputs.rs`).** Statistics could not help: Burrmill's scans
set no per-file statistics, which is why turning them on changed nothing. But the catalogue knows
every segment's size, so a round-robin repartition is removed where the scans beneath it read less
than 4 MiB. It only spreads work over threads, so no answer can change; DataFusion's own sanity
check is rerun and the rule backs off if it fails. Over the nest, 1 to 64 MiB were all within noise
(0.622-0.652x) of off (0.646x), except for small statements: `lodestar_disputes` 19 ms to 7-11,
now **0.80x DuckDB**. The fuzz fixture is small enough that the rule applied to nearly all of
10,000 queries, with no answer changed. Serving all 22 views, Burrmill alone, 1/4/16 clients:
2.2/4.4/3.4 qps off and 2.3/4.9/3.3 on, the same. It exposed a fault in `RangeJoinExec`, which kept
the nested loop's partition count when its children changed; it now takes it from its right child.

---

## Fuzzing, widened: time, regex, strings — and a DuckDB bug — 2026-09-25

The grammar now draws what agents write against nests and had never been generated: timestamps
from `block_timestamp`, `date_trunc`, `extract`/`date_part` and the shorthands (`year()`,
`dayofweek()`, `epoch()`), `strftime`, `date_diff`, `DATE ± INTERVAL`; `regexp_matches`,
`regexp_extract`, `regexp_replace`, `ILIKE`, `split_part`, `trim`, `lpad`, `concat_ws`;
`rank`/`dense_rank`/`ntile`/`lead` and frames; `string_agg`, `bool_and`; `DESC`, `NULLS FIRST`,
`OFFSET`. The first run: **656 of 2,000 refused, 32 different**. Now: **3,000 and 5,000 cases on two
seeds, no difference but two from a DuckDB bug, nothing looser.** Fixed:

- **NULL ordering.** DuckDB sorts NULLs last in both directions; DataFusion puts them first under
  `DESC`, so `ORDER BY x DESC LIMIT n` returned other rows. NULLS LAST is now written wherever an
  ordering does not say (queries, aggregates, windows).
- **Missing functions**, each to DuckDB's measured behaviour: `strftime` with DuckDB's own
  specifiers (`%f` is microseconds, `%g` milliseconds, not chrono's), `date_diff`/`datediff`
  counting calendar boundaries, `regexp_extract` (`''` where nothing matches),
  `regexp_replace` (first match unless `g`; a rewrite naming a missing group leaves the string,
  as RE2 does), `sign` as TINYINT, and the date-part shorthands and `regexp_matches` as renames.
- **Timestamps as text**: `2024-01-01 00:00:00+00`, fraction trimmed, where arrow wrote ISO `T`/`Z`.
- **Date arithmetic types**: `date_trunc` of a DATE and `DATE ± INTERVAL` are TIMESTAMP (the
  latter had kept the DATE and dropped the hours), `DATE + 3` stays a DATE.
- **Result types of mixed values**: a DOUBLE among `CASE`/`COALESCE`/`greatest`/`least`/`NULLIF`
  values makes all DOUBLE; mixed-sign integers there take DuckDB's union type (HUGEINT beside
  UBIGINT), and `NULLIF` fits its literal.
- DuckDB refuses a string or decimal literal as an `ORDER BY` key; so does Burrmill.

**A DuckDB 1.5 bug**, the two remaining differences: with a date part projected, a comparison
between text and a cast timestamp drops every row —
`SELECT year(t) FROM (SELECT to_timestamp(1700000000 + x) AS t FROM range(10) r(x)) WHERE 'bob' <> CAST(t AS VARCHAR)`
returns nothing, while projecting `t` returns all ten. Burrmill keeps the rows. Recorded as
`KNOWN` in `dialect-parity` (119/119); not reported upstream, which is Chief's call.

Owed: `lodestar_disputes` is a few milliseconds of mostly planning, 2-4x DuckDB's 3 ms when timed
alone, and reads 0.8-1.7x in `engine-views` run to run; an A/B against this morning's build shows
no regression. Why a small statement costs milliseconds to plan is worth finding, since every
`/sql` call pays it. The nest: 22/22 identical, 0.641x.

---

## Differential fuzzing — nine faults, three of them silent wrong answers — 2026-09-25

Slice 1 owed a generated corpus against DuckDB. `burrmill-bench fuzz` draws queries from a typed
grammar (integer, text, decimal and boolean expressions; `CASE`, `COALESCE`, casts and `TRY_CAST`,
`LIKE`, `IN`, `BETWEEN`, `IS DISTINCT FROM`; aggregates with `GROUP BY`/`HAVING`; joins, derived
tables, CTEs, scalar/`IN`/`EXISTS` subqueries, windows, all six set operations) over a seeded
fixture chosen to be awkward: mixed-case and NULL text, UBIGINT keys as nests have them, negative
and missing amounts, integer text past 64 bits and text that is no number. Every query is
deterministic; rows compare as a multiset unless the query orders by every column. Outcomes are
counted apart: identical, both refuse, the checked rule's designed refusals, designed exact sums
(DuckDB rerun with those casts widened agrees with Burrmill), overflows that depend on each
optimiser's rewrites, stricter, looser, and different.

First run, 500 cases: **336 identical, 18 different, 138 stricter**. After the fixes, **3,000
cases on that seed and 5,000 on a fresh one: no different answers, nothing looser.** What it found:

- **Silent wrong answers.** `TRY_CAST('0xa' AS BIGINT)` was NULL where DuckDB reads 10, emptying
  whole filters. `EXCEPT ALL`/`INTERSECT ALL` were set operations, not bag ones (DataFusion plans
  them as anti/semi joins): now rows are numbered within their duplicates on both sides and joined
  on that number too. And one of my own, caught the same afternoon: the exact sum over `TRY_CAST`
  read `0x` text with HUGEINT's grammar, so `sum(TRY_CAST(hex AS BIGINT))` came out NULL.
- **Text to integer, as DuckDB reads it**: trimmed, signed, underscores, fractions and exponents
  rounded half away (`'1.5'` is 2, `'5e-1'` 1), in one grammar (`wide::duck_number`) shared by the
  casts and the exact sums. Text that is no number is NULL under `TRY_CAST`, and no longer refuses
  an exact sum.
- **`avg` is DOUBLE** for every exact input, as DuckDB's is: exact sum, DuckDB's hugeint-to-double
  rounding, over count times 10^scale. It had been a DECIMAL (`"18.0000"`).
- **Integer types across signedness**: `//` keeps UBIGINT and goes HUGEINT beside a signed type;
  `greatest`/`least` fit a literal to their other argument; `INTERSECT`/`EXCEPT` take DuckDB's union
  type (HUGEINT for mixed signs, DataFusion's resolution beside a DECIMAL). What JSON prints, a
  number or a string, follows from these.
- **Function aliases**: only primary names were registered, so `length` (DataFusion's alias of
  `character_length`) and every other alias was refused. The function audit still passes.

Left stricter, by count: `EXISTS`/`IN` subqueries inside a `SELECT` list (184 of 5,000), which
DataFusion plans only as filter predicates. A refusal, not an answer. `dialect-parity` 107/107
with the fuzz finds pinned; one `KNOWN` (an overflow DuckDB's optimiser rewrites away). The nest:
22/22, 0.653x. Transcript: `docs/bench/fuzz.txt`.

---

## Subquery column names, and a silent loss of columns — 2026-09-25

The owed "nested-SELECT duplicate unaliased names" turned out to hide a wrong answer.
`SELECT * FROM (SELECT * FROM t a JOIN t b ON ...)` **returned half its columns on Burrmill,
without an error**: an unaliased subquery passed both sides' `from`, `to`, ... through under one
name each, and the result encoder, which keeps one value per name as nuthatch's does, kept one.
DuckDB, inside a subquery or CTE, renames a repeated column `from_1` and names an unaliased
expression by its text, `(x + 0)`, and the outer query may use either. DataFusion refused the
explicit repeats, or renamed them `from:1` where the subquery had an alias.

`df/subqueries.rs` walks each derived table and CTE bottom-up for DuckDB's output names:
aliases, printed names for unaliased expressions, wildcards expanded from the tables' and views'
columns (`Known` now carries them) or from an inner subquery already named. Repeats are renamed by
DuckDB's rule, measured: `_1`, `_2` until free, compared without case, the renamed column keeping
its spelling (`a, A` is `a, A_1`), column-alias lists first (`t(x)`). The select list is rewritten
only where a name changes; `USING`/`NATURAL` and semi joins, table functions, wildcard options and
expressions the printer cannot name leave the query as it was.

`dialect-parity` 82/82 with nine new cases; a regression test in `df_engine`. On the thinkpad
nest, 22/22 identical, 0.645x, all within 1.5x.

---

## Dialect: booleans beside numbers, decimal literals, `round` — 2026-09-25

`dialect-parity` gained 29 cases (73 now, all identical to DuckDB), covering today's fixes and the
literal typing they exposed:

- **A boolean compared with a number is the number**, `true` as 1: `2 = true` is false, `2 > false`
  true, `true IN (1, 2)` true, all measured on DuckDB. Done in an `ExprPlanner`
  (`plan_binary_op`), because DataFusion types a `SELECT` list while converting the SQL and refused
  `Int64 = Boolean` before any analyzer rule could act; `IN` lists stay in `DuckComparisons`.
- **Decimal literals.** DuckDB types `1.5` as DECIMAL(2,1) and nuthatch prints it `"1.5"`;
  Burrmill had it a DOUBLE, `1.5`. No corpus case returned a bare decimal literal, which is how it
  survived. DuckDB counts digits as written (`0.5` is DECIMAL(2,1), `.5` (1,1), `00.50` (4,2)),
  exponents are DOUBLE, and past 38 digits DOUBLE; DataFusion drops the leading zero and gives
  `1e3` a negative scale. The dialect now writes each literal as an exact cast of its own text.
  The encoder also no longer panics on a negative scale, which it did.
- **`round` on a DECIMAL** is whitelisted: DataFusion's checks the rounded value against the
  precision and refuses, and its types agree with DuckDB's on the cases tried (`round(9.995, 2)`,
  no digits, more digits than the scale, negative digits).

`error-parity` 14/14, `encode-parity` 17/17, `reach-parity` unchanged. On the thinkpad nest: 22/22
identical, 0.65x time-weighted, all within 1.5x, as before.

---

## 6.0b — the footprint, re-measured on Burrmill itself — 2026-09-25

6.0 measured a stand-in: component crates and a ported planner in a nuthatch-shaped consumer. Gate
1 asks that those figures hold, so `probes/footprint6` gained a fourth variant, `burrmill`, the same
consumer (six test binaries, four querying, `line-tables-only`, thin-LTO stripped release) on
`burrmill::Engine` with its `datafusion` feature. Same machine, same toolchain, same answer (90
parties, first balance 3185):

| | duck | umbrella | components (6.0) | **burrmill** |
|---|---:|---:|---:|---:|
| querying test binary | 162 MB | 493 | 446 | **495** |
| release binary | 41 MB | 127 | 105 | **112** |
| target directory | 3.27 GB | 4.48 | 4.14 | **4.76** |
| clean test build (j32) | 77 s | 121 | 87 | **84** |
| incremental | 1.1 s | 1.9 | 1.9 | **1.8** |

**They do not quite hold: +11% on the test binary, +7% release, +15% target**; build times are
level or better. The test binary's code is 10.2 MB larger, and every part of it traces to a feature
added on purpose since 6.0: sqlparser's AST visitor (+2.0 MB, `reach` and the dialect rewrites),
the nested list functions (+1.7, `list_reduce`), `stacker` (+1.5, `recursive_protection`), generic
instantiations under `core` and `datafusion_expr` (+4.7), Burrmill's own code (+0.9). Against DuckDB
the querying test binary is now 3.05x, from 2.75x. Whether that trade is acceptable is the RFC-0042
amendment's question, and Chief's; nothing here is waste to be cut without giving up a feature.
Results: `probes/footprint6/results/burrmill.txt`.

---

## 6.6d — the memory gate: the gap was code, and it passes as nuthatch would run it — 2026-09-25

6.6c left the hosted fold at 249-261 MB against a 256 MB gate, 20-40 MB over the bare operator,
"the last 5-20 MB owed from inside the engine". Measured today on the same fixture (989,690
groups, streamed `CAST`, one fold per process) it reads 247-260 in the bench binary. Before
changing anything, the question was what those megabytes are.

- **Not data.** A counting global allocator (`burrmill-bench --features alloc-stats`) gives the
  peak of live heap: **163 MB bare and 163-164 hosted**. The hosted fold holds nothing more.
- **Not the allocator either.** Sampling `/proc/self/statm` every 2 ms and keeping the largest
  reading splits it: anonymous memory at the peak is **188-211 MB hosted against 207-214 bare**.
  The difference is **file-backed: 55 MB against 21**, the binary's own code, which the hosted path
  runs more of.

The bench binary links DuckDB and the umbrella DataFusion as oracles, 216 MB of it, so the code a
query touches is spread thin across it and each page drags in its neighbours. That is not what
ships. **`examples/hosted_fold`** is the same query in a binary with Burrmill and its `datafusion`
feature and nothing else, 130 MB:

| | VmHWM | file-backed | anonymous |
|---|---:|---:|---:|
| **hosted fold, lean binary** | **241-245 MB** | 48 | 195-198 |
| hosted fold, bench binary | 244-267 | 55 | 188-211 |
| bare fold, bench binary | 211-236 | 21 | 207-214 |

**4/4 under 256 MB, with 11-15 MB spare, no allocator setting.** The latencies printed beside are
cold single folds and are not comparable with the warm medians above. Of the anonymous 195 MB,
160 is live; the rest is allocator slack and thread stacks, and is where any further margin would
come from. Transcript: `docs/bench/memory-gate-thinkpad-2026-09-25.txt`.

---

## Range joins — `lodestar_epochs` from 1.92x to 0.5x — 2026-09-25

Phase 1b left one view over 1.5x: `lodestar_epochs`, whose four joins of events to epochs by block
range (`x >= start AND x <= until`) ran as nested loops, 574-711 ms of CPU each. DataFusion 55
has no range join. **`RangeJoin`** (`df/rangejoin.rs`) is a physical rule, last in the list, that
replaces a `NestedLoopJoinExec` of that shape with `RangeJoinExec`, keeping what the nested loop
did and promised: the left side collected once, the right streamed by partition, the same columns,
the same plan properties.

The filter normalises to `L_a >= R_a AND L_b <= R_b`, strict or not, written either way round. The
left rows are sorted by `L_a`; each right row binary-searches its first candidate and scans on
until a suffix minimum of `L_b` shows nothing further can match. Events against epochs, whichever
side is which, cost one search and one short scan per row, and overlapping intervals stay correct,
only slower. Taken for inner and right joins (a left join is planned as a right one), plain column
operands, one non-float type per comparison; NULL matches nothing.

`tests/df_rangejoin.rs` checks it against brute force in plain Rust: 3,000 points, 60 intervals
with overlaps, empty and NULL bounds, all four strictness combinations, both table orders, the
conjuncts reversed, and the outer join keeping unmatched points. Two planted faults, ignoring
strictness and stopping the scan at the first non-match, both fail it.

**On the thinkpad nest: `lodestar_epochs` 2,564 ms to 682-715 (DuckDB 1,343-1,359), all four
joins converted, 22/22 still identical. Time-weighted 0.65x DuckDB and 22/22 within 1.5x.** No other
view contains the shape. `deployment_signal` read 1.48x in one full run; an alternating A/B against
the build before today's engine work gave 10-12 ms for both, the same median, so that was the
harness. Transcript: `docs/bench/rangejoin-thinkpad.txt`.

---

## Phase 1b — every graph-allocations view on Burrmill, byte for byte — 2026-09-25

Gate 1's parity leg wanted every authored statement to reach parity once the DuckDB-only views were
rewritten. **22/22 now do**, on the thinkpad's 1,925-segment copy: each rewrite gives DuckDB the
same rows as the original, and Burrmill gives the same rows as DuckDB (`rewrite-parity`, byte
identical in nuthatch's JSON). The two zero-row checks pass on both engines. The two pinned checks
agree between the engines but not with their fixtures, which were taken on 19 August from a
smaller nest.

**Time-weighted 0.69x DuckDB** (27.5 s against 39.6 s over the 22), and **21/22 within 1.5x**.
Transcript: `docs/bench/phase1b-thinkpad.txt`.

### The nest side (branch `pete/portable-views`, local on the thinkpad, not pushed)

Seven commits, each checked by `rewrite-parity` before the next. ASOF joins become a newest key
carried forward over a union in key order, then an equality join; `LATERAL` becomes one more level;
a `FILTER` subquery becomes a scalar subquery; list comprehensions over a hex word become two
64-bit halves, `CAST('0x' || substr(h, 1, 16) AS UBIGINT)`, since DuckDB casts `0x` text to UBIGINT
but not to HUGEINT. The first version wrote the 32-digit fold out term by term; it planned in
532 ms against DuckDB's 82 and was replaced.

### The engine side

Two views, `lodestar_delegator_stakes` and `lodestar_delegators`, now run **unchanged**:

- **`list_reduce`**, DuckDB's, as a DataFusion higher-order function (`df/lists.rs`). One batch
  evaluation of the lambda per list position, rows ordered longest first so the live rows are a
  prefix; NULL list NULL, empty list refused as DuckDB refuses it, the result cast back to the
  element type. DataFusion's nested list functions and planners are registered with it, and
  `list` as `array_agg`.
- **`list_prepend`/`list_append` of a struct.** DataFusion unifies the element and the list's
  elements, then refuses to cast a struct to that type; `Utf8` against `Utf8View` was enough.
- **`acc.rate` inside a lambda.** DataFusion resolves a lambda parameter only by its bare name;
  the dialect rewrites `p.f` into a field access on `p`.

Then the fold agreed on shape and disagreed in the last digits, 16,384 wei on 6.4e18. Two
roundings, both found by measuring DuckDB rather than reading it:

- **DECIMAL to DOUBLE.** DuckDB converts a hugeint as `lower + upper * 2^64`, rounding twice, and a
  scaled decimal past 2^53 as integer part plus fraction. arrow rounds once, correctly, and
  disagreed on 24 of 3,011 sampled HUGEINTs. `DuckDoubles` models DuckDB and matched it on all
  3,011 and on 1,458 DECIMAL(38,9)s.
- **DOUBLE to HUGEINT is half to even** (`nearbyint`), while DOUBLE to DECIMAL(38,0) is half away.
  Both are DECIMAL(38,0) here, so a HUGEINT cast now carries a marker until the analyzer has seen
  its source type.

### Found on the way

- **`a IS DISTINCT FROM b OR c ...`** parsed as `a IS DISTINCT FROM (b OR c ...)`: sqlparser reads
  that operand at the lowest precedence. Both zero-row checks failed on it. The DuckDB dialect is
  now wrapped to parse it at `IS`'s precedence, as DuckDB and Postgres do.
- **`SUBSTRING` only planned in the bench.** datafusion-sql plans it only with its
  `unicode_expressions` feature, which Burrmill never named; the bench's stock arm pulls the
  umbrella crate, and unification lent it. A consumer depending on Burrmill alone would have been
  refused. Burrmill names it now, with `recursive_protection`, which the bench had also been
  lending and without which a deep enough expression aborts the process instead of the query.
- **engine-views swallowed its own error** in a tokio panic from dropping the engine inside `main`'s
  runtime; the error was a check with no fixture, which means zero rows.

### Owed

- ~~**`lodestar_epochs`, 1.92x.**~~ Range joins, above: 0.5x.
- Planning is quadratic in expression depth (a 32-term sum: 66 ms). DataFusion recomputes a type
  from the whole subtree whenever asked; the analyzer passes ask at every node. **2026-09-25:**
  `DuckComparisons` now asks only where a literal is fitted or a comparison typed, 66 to 59 ms.
  The rest is `CheckedArithmetic` (~11 ms, which needs every operand's type) and DataFusion's two
  coercion passes (~30 ms), quadratic for the same reason inside DataFusion. Carrying types
  through the checked rewrite would buy at most the 11 ms, on the rule that guarantees exactness,
  and leave the larger half; not done. No view pays it since 7092337.
- HUGEINT is DECIMAL(38,0), which stops at 10^38 - 1 where HUGEINT reaches 2^127 - 1. A value
  between refuses; it does not answer wrongly.
- An integer compared with a boolean (`1 = false`): DuckDB casts, Burrmill refuses.

---

## 6.2 closed — the function surface audited; one path leak removed — 2026-09-25

6.2 left "a function allowlist built from the census" open. The census counts features, not
function names, and the reason for an allowlist on DuckDB was functions that reach outside the
query: `read_csv`, `getenv`, `glob`, the `duckdb_*` catalogue. So the question asked was whether
DataFusion's registry, as the engine builds it, has any such function at all.

`tests/df_functions.rs` lists all 172 scalar, aggregate and window functions (`Engine::
function_names`) and fails on any name suggesting files, environment, network, catalogue or query
execution. **Two turned up:**

- **`input_file_name()` returned the server's full segment path.** Measured on the unpatched
  engine: `private/var/folders/.../segments/t-0000...0001.parquet`. That is the class of nuthatch's
  audit finding 2 (`duckdb_settings()` path disclosure), and nuthatch rewrites its nest directory to
  `<nest>` in error text for the same reason. Removed.
- **`file_row_index()`**, a row's position within its file. File-identity information that no view
  uses and DuckDB does not offer. Removed.

Calling either is now refused without the path appearing in the error (tested). Nothing else
reaches outside the query. **A census allowlist would therefore close no further hole, and it would
refuse ordinary functions agents use.** The audit test stands in its place, and fails if a
DataFusion upgrade brings in something of that kind.

---

## 6.7 — what a statement reaches, answered by sqlparser — 2026-09-25

nuthatch asks DuckDB's parser (`json_serialize_sql`) which tables and table functions a `/sql`
statement touches, refuses anything unrecognised, and bounds its integrity sweep by the tables
reached. **`burrmill::inspect::reach`** answers the same from sqlparser's AST. It is in the default
build, with no DataFusion needed; sqlparser's `visitor` feature is now on there, which adds one
small proc-macro crate. The rules are nuthatch's:

- table functions `generate_series`, `range` and `unnest` only;
- base tables named `[A-Za-z0-9_]+`, which is what stops `FROM '/etc/passwd'`;
- a schema other than `main` surveys the catalogue;
- tables reached, lowercased, CTE names included.

It is stricter where nuthatch's walk is open: an unparseable statement, more than one statement, or
anything but a query is refused, not passed to other guards.

`burrmill-bench reach-parity` runs 46 statements through nuthatch's own walk (copied, over DuckDB's
`json_serialize_sql`) and through `reach`. **38 identical, 8 stricter, none looser.** The hostile
cases (`read_csv` bare and quoted, `glob`, `duckdb_settings()`, `FROM '/etc/passwd'`, a keyword
inside a literal or a comment) are refused alike. The stricter ones are `COPY`, `ATTACH`, `INSTALL`,
`PIVOT` and `AS OF`, which DuckDB will not serialise, so nuthatch's walk does not decide them; two
stacked statements; and `ASOF`/`TABLESAMPLE`, which sqlparser cannot parse. Transcript:
`docs/bench/reach-parity.txt`.

Not done: the other `json_serialize_sql` consumers, which are graft's canonical plan, the entity
shape gate, the Dune translation and entity lowering. They depend on DuckDB's own AST shape and
belong with phase 2's engine trait.

---

## 6.8 — concurrency on the DataFusion path, on real views — 2026-09-24

Two sweeps on the thinkpad, 8 threads per query, worst client's own p99 and fairness
(slowest client's count over the fastest's), as in 5.2.
Transcripts: `docs/bench/serve-engine-thinkpad.txt`, `docs/bench/serve-views-thinkpad.txt`.

**First run, and a starvation found.** `Engine` had no admission gate. At 32 clients sharing one
runtime, one client was served nothing, and the worst p99 was 2.9 s. This is 5.3's bug on the new
path. The engine now takes the same FIFO `Gate`, half the thread budget wide, around execution
only (planning stays outside).

**The synthetic fold** (2M rows, 10k parties, 64 segments; parity 9,896 parties across all arms):
the engine now matches the owned fold. Fairness is 0.91 at 16 clients and 0.83 at 32, and the
worst p99 is 664 and 1,147 ms, level with DuckDB's own 577 and 1,128. DuckDB with a connection
per client gets more throughput (48-50 qps against 29-32) by being unfair (0.30-0.46), the trade
5.2 recorded.

**nuthatch's real views** (the 12 graph-allocations views both engines answer identically; each
client cycles through them). DuckDB is at its best here, one connection per client; Burrmill is
one shared `Engine`:

| clients | DuckDB qps | Burrmill qps | DuckDB worst p99 | Burrmill worst p99 | fairness D / B |
|---:|---:|---:|---:|---:|---|
| 1 | 7.2 | **14.7** | 507 ms | **255 ms** | 1.00 / 1.00 |
| 4 | 14.8 | **34.6** | 1,414 ms | **527 ms** | 0.88 / 0.97 |
| 16 | 17.0 | **32.7** | 5,236 ms | **1,109 ms** | 0.58 / 0.89 |
| 32 | 15.1 | **32.9** | 7,424 ms | **1,350 ms** | 0.00 (one starved) / 0.90 |

- **At 32 clients, DuckDB fails** under the default `ulimit -n` of 1,024: `Too many open files`,
  from 32 connections each opening segments. The 32-client row is with 65,536 for both engines.
- **Peak RSS, one engine per process, 32 clients: DuckDB 15.4 GB, Burrmill 3.9 GB.** Each process
  also ran the one-time parity pass on both engines, which the gap swamps.

On the workload nuthatch actually serves, the DataFusion path with the owned rules is about twice
DuckDB's throughput at every client count, with a quarter to a fifth of its tail latency and a
quarter of its memory at 32 clients.

---

## Gate 1 speed — repeated subqueries computed once, and the floor found — 2026-09-24

- **`ShareRepeats`** (`src/df/sharing.rs`). An aliased subquery that occurs more than once and
  contains an aggregate is computed once, behind a shared cache (`OnceCell`), and read by every
  use. Its node lets nothing through from above, so every copy is optimised alike. It only shares
  aggregates, whose output is bounded, rather than buffering whole scans. `epoch_boundaries` uses
  `per_epoch` twice: **34 → 20 ms (1.11x)**.
- **`NOT IN` left alone, measured.** A plain anti-join guarded by one `count(*)`/`count(y)` over
  the subquery gives the same rows in 77 ms against the null-aware join's 38 (DuckDB: 43 against 24).
  The null-aware join is the better plan.
- **The floor.** `SELECT 1` is under a millisecond, so there is no fixed cost. `lodestar_disputes`
  (8 rows, DuckDB 3-4 ms) spends 6 of its 9 ms planning a large inlined view tree. Timed per rule,
  that is about 2.3 ms in the analyzer (Burrmill's rules about 0.8 of it) and about 2.5 ms in
  DataFusion's two optimizer passes. No single pass is worth attacking.

Real nest, warm medians, **12/12 byte-identical**: **0.53x DuckDB time-weighted** (814 ms against
1,538). **9 of 12 within 1.5x** per statement. Over: `open_allocations` 1.78x (41 ms against 23)
and `port_queue` 1.70x (51 against 30), both the null-aware anti-join; and `lodestar_disputes`,
9 ms against 3, which is planning.

---

## Gate 1 speed — the text-to-HUGEINT cast, and where it stops — 2026-09-24

The two small shapes, profiled. `epoch_boundaries` was not its window. It was four scans of about
30 ms each: two tables read twice, because DataFusion inlines a CTE referenced twice, and nearly
all of each scan was `CAST(text AS DECIMAL(38,0))` in Arrow's general string parser.
**`FastTextCasts`** (`src/df/fastcast.rs`) parses text that is plainly an integer (optional `-`, at
most `p` significant digits) straight to i128. Everything else, errors included, goes to Arrow's
own cast for that value, so the answer is Arrow's by construction. It leaves `TRY_CAST` alone,
because the checked arithmetic rule finds lossy values by it, and it runs after the rules that
match `CAST`. The views cast text to HUGEINT at 305 sites, so the gain is broad.

Real nest, warm medians of 5, **still 12/12 byte-identical**: **0.55x DuckDB time-weighted**
(840 ms against 1,536). `lodestar_allocations` 242 against 298, `epoch_boundaries` 34 against 18,
and `deployment_signal` is now within 1.5x (1.38).

Per statement, **8 of 12 within 1.5x**. Still over:

- `open_allocations`, 1.62x: a null-aware anti-join from `NOT IN`, whose hash table DataFusion
  builds on one thread;
- `port_queue`, 1.73x: built on it;
- `epoch_boundaries`, 1.89x: the CTE read twice;
- `lodestar_disputes`: 11 ms against 4, which is noise at that size.

`NOT IN` was left as it is. A parallel anti-join keeps DuckDB's answer only with NULL and
empty-subquery guards, which add two more scans of the subquery. That is not clearly a win against
a 15 ms gap, and it is a correctness risk bought for speed. Materialising a CTE referenced twice is
the other candidate. It is DataFusion's to do, or Burrmill's with a shared scan.

---

## Gate 1 speed — two plan shapes owned, and the time-weighted leg passes — 2026-09-24

Profiled rather than guessed (`burrmill-bench engine-analyze`, `EXPLAIN ANALYZE` through `Engine`).
The loss was concentrated, not spread.

**1. The latest row per key.** `ROW_NUMBER() OVER (PARTITION BY k ORDER BY o) = 1` beside
`SUM(x) OVER (PARTITION BY k)`, over a 768,900-row union, was 1.4-2.2 s of window compute in each
of three views. DataFusion's window operators are slow on many small partitions, and Burrmill's
checked window sum is not the cause (native: 754 ms against 807). By hand, the same 758,271 rows
took 628 ms as windows and 178 ms as a `GROUP BY` with `first_value(... ORDER BY o)`, against
DuckDB's 191 ms, with the same digest. **`TopPerGroup`** (`src/df/topn.rs`) plans it that way:
the filter `rn = 1` directly or under an alias, `ROW_NUMBER` plus partition-wide aggregates on the
same keys, columns only above. Anything else is left alone.

**2. `COUNT(DISTINCT x)` beside other aggregates**, which DataFusion keeps as per-group sets in
both phases. **`DistinctSplit`** (`src/df/distinct.rs`) groups by `(k, x)` and then by `k`, keeping
names and types. My hand version first re-summed a `COUNT(*)` into a HUGEINT, which nuthatch prints
as a string, and the digest caught it. The rule casts counts back.

Also fixed on the way: `EXPLAIN` of DuckDB-dialect SQL skipped the rewrites, because DataFusion
parses `EXPLAIN` into its own statement type.

Real nest, warm medians of 5, 8 threads, **still 12/12 byte-identical**:

| view | before | after | DuckDB |
|---|---:|---:|---:|
| lodestar_allocations | 845 | **277** | 304 |
| lodestar_deployments | 396 | **151** | 485 |
| lodestar_provisions | 549 | **148** | 243 |
| lodestar_curator_signals | 349 | **124** | 255 |
| deployment_signal | 33 | **12** | 8 |
| open_allocations | 68 | **41** | 22 |

**Time-weighted: 1.54x → 0.60x DuckDB. That leg of gate 1 passes.** Per statement, 8 of 12 are
within 1.5x. The other four are all under 60 ms: `open_allocations` 1.86x (an anti-join from
`NOT IN`), `epoch_boundaries` 2.63x (a `LAG` over a small join), `port_queue` 1.58x (built on
both), and `lodestar_disputes`, 7 ms against 3. Each is its own shape and is owed separately.
Transcript: `docs/bench/engine-views-thinkpad.txt`.

---

## Gate 1 on real data — parity holds, and the speed claim does not — 2026-09-24

`burrmill-bench engine-views <nest>` runs every authored view of a real nest through `Engine`,
against DuckDB set up as nuthatch sets it up (`union_by_name`, `_dec` as `DECIMAL(38,0)`,
`_overflow`, UTC). It compares each view's whole contents as a multiset of nuthatch-encoded rows,
then runs the nest's pinned checks. The nest is the thinkpad's copy of graph-allocations
(`~/1165-corpus/graph-allocations-nest-next`, 1,925 segments, 643 MB), so no RPC was needed.
Transcripts: `docs/bench/engine-views-thinkpad.txt`, `docs/bench/df-views-thinkpad-1925seg.txt`.

**Parity: 12 of 12 portable views are byte-identical**, including 260,828 and 608,700-row views.
The other 7 are DuckDB-only syntax (2 `ASOF`, `list_reduce`, list comprehensions) or depend on
one. That is phase 1b, nuthatch's own views. Getting there took four engine fixes, each measured
against DuckDB first:

- `register_view`, and `_overflow` beside `_dec`, with taint stopping at `IS [NOT] NULL`: whether
  a `TRY_CAST` came back NULL is an exact boolean;
- DuckDB's integer-literal typing: `UBIGINT - 1` stays UBIGINT, where DataFusion widens to
  `DECIMAL(20,0)` and nuthatch then prints a string;
- `ORDER BY s.x` beside `... AS x`, which DataFusion refuses as ambiguous;
- casts to integer round as DuckDB's do: half to even from a float (`2.5` → 2), half away from
  zero from a decimal (`2.5` → 3). Arrow truncates.

**Checks:** Burrmill and DuckDB agree on both. Neither matches the pinned JSON (14,654 against
13,884 deployments), because this data copy is newer than the fixture.

**Speed: gate 1 fails, and the cause is DataFusion, not Burrmill.** Warm medians of 5, 8 threads:
Burrmill is **1.54x DuckDB** time-weighted. It wins 3 of 12 (`lodestar_delegations` 0.27x,
`escrow` 0.44x, `deployments` 0.81x) and loses the rest by up to 4.7x on small views. The control
is investigation 04's own `df-views`, stock DataFusion on the same nest and machine: **1.81-1.87x**.
The engine's own layers (checked arithmetic, dialect, provider) are therefore not the cost. Planning
is 2-49 ms. The time is in execution above the scan: joins and aggregation.

**Investigation 04's 0.71x was measured on a 38,428-segment copy**, where DuckDB pays per file.
On 1,925 compacted segments DuckDB is the faster engine by nearly 2x. The plan's "speed is not the
obstacle" rested on that layout, and it does not hold on this one. That is Chief's to weigh before
anything else is built on it.

---

## 6.5 closed — identifiers without case, `information_schema`, repeated names — 2026-09-24

- **Identifiers resolve without regard to case, quoted or not, as DuckDB resolves them.** Before
  planning, an identifier with no exact match is rewritten to the one table, column or alias it
  matches ignoring case. An ambiguous one is left for DataFusion to reject. Result names follow
  DuckDB: a bare column takes the catalogue's case (`"Value"` → `value`), and a column inside an
  expression keeps its case as written.
- **`information_schema.tables` and `.columns`**, as nuthatch's `.tables` and `.schema` read them:
  catalogue `memory`, schema `main`, every table a `VIEW` as nuthatch's are, DuckDB's type names,
  and the `__raw` tables behind `_dec` hidden. This is a subset of DuckDB's columns (4 of 13, 7 of
  46): the ones those commands use.
- **Repeated result names** (`SELECT *, value_dec`, `SELECT 1 AS a, 2 AS a`), which DuckDB allows
  and DataFusion's projection refuses. Repeated items get a private suffix before planning, and
  lose it on the result.
- **Window functions** are named as DuckDB names them. The names unit test grows to 34, every
  string taken from DuckDB.

`dialect-parity` **44/44** (39 byte-identical, 5 refused by both). `error-parity` has no known
differences left: case-insensitive names closed the last one, and the DuckDB dialect had already
closed `SELECT from FROM t`.

---

## 6.5 — DuckDB's dialect, measured statement by statement — 2026-09-24

`burrmill-bench dialect-parity` runs 36 statements, drawn from the census's most common
constructs, through DuckDB (views built as nuthatch builds them) and through `Engine`. Each result
is encoded as nuthatch encodes it and compared byte for byte. The first run matched 16/25. It now
matches **36/36: 31 byte-identical, and 5 refused by both engines where DuckDB refuses.**
Transcript: `docs/bench/dialect-parity.txt`.

What it took:

- **Parsing with sqlparser's DuckDB dialect**, then rewriting the AST: `HUGEINT` → `DECIMAL(38,0)`,
  `UBIGINT` → `BIGINT UNSIGNED` (`UHUGEINT` refused), tuple comparisons expanded, and `a // b` →
  `burrmill_intdiv`, which is exact and truncating. Built on `/`, it would have become a float once
  `/` was made DOUBLE. `NOT x` → `x = false`, because DataFusion's SQL planner type-checks `NOT`
  before any rule could cast text. The dialect switch also made `SELECT from FROM t` a syntax error,
  as DuckDB has it, which closed a 6.7 item by accident.
- **`/` is DOUBLE**, as in DuckDB (`DuckSemantics`, after coercion). `7/2`, `1/0` and
  `-7 // 2` all agree.
- **`to_timestamp` is microseconds, UTC.** nuthatch sets no time zone, so its `date_trunc`
  follows the host's: the same nest gives different day boundaries on a UTC server and on the
  MacBook. Burrmill is UTC, and the harness sets DuckDB to UTC, as a server runs it.
- **`extract`** needed DataFusion's datetime expression planner registered.
- **Text beside numbers and booleans** (`DuckComparisons`, before coercion, because afterwards a
  written `CAST` and an implicit one look the same). Measured: DuckDB casts text for `=` and `<>`,
  refuses `<`, `>`, `<=`, `>=` and `BETWEEN` against any number, and casts text to BOOLEAN beside a
  boolean and under `AND`/`OR`/`NOT`. All reproduced, including the refusal wording.
- **`_dec` sums read text as DuckDB's `TRY_CAST` does, where that is still exact.** Leading zeros,
  sign, surrounding spaces and a zero fraction are accepted; `7.9`, which DuckDB rounds, and
  exponents are refused.
- **Default column names**, which are nuthatch's JSON keys (`count(*)` is `count_star()`). DuckDB
  prints the parsed expression. `src/df/names.rs` prints the measured forms from the statement as
  written, and matches 28/28 in its unit test and every unaliased column in the corpus. The
  quoting rule comes from DuckDB's own keyword list (489 words, generated), and DuckDB folds
  `NOT (a > b)` to `(a <= b)` before naming. Forms it does not cover keep DataFusion's name.

Still open in 6.5: quoted identifiers are case-insensitive in DuckDB (`"Value"` finds `value`),
and `information_schema` shaped for the REPL. DuckDB-only syntax (ASOF, list comprehensions,
`list_reduce`) is phase 1b, in nuthatch's own views.

---

## 6.4 — nuthatch's JSON and nuthatch's error classes, checked against nuthatch's own code — 2026-09-24

**Encoder** (`src/df/encode.rs`). nuthatch encodes a cell as duckdb-rs's `ValueRef`, which
duckdb-rs derives from the Arrow type alone, and then `value_to_json`. So the encoding is a
function of the Arrow type, and this ports it: numbers for every integer width, float numbers,
digit strings for scale-0 decimals, and `Debug` text for timestamps, dates, times, intervals and
blobs. Scaled decimals are DuckDB's own VARCHAR cast, since nuthatch now casts them before encoding
(#1433). That cast has a quirk: with precision equal to scale, DuckDB writes no integer digit
(`.5`, `-.05`). The first differential run found it, and nobody would have guessed it.
`Utf8View` encodes as text. Nested types are refused, because their `Debug` prints the whole Arrow
column rather than the value.

`burrmill-bench encode-parity` runs a 17-query typed corpus. nuthatch's side is its own code:
`value_to_json` copied verbatim at 5513498, plus the #1433 rewrite. Burrmill's side takes DuckDB's
own Arrow batches through IPC and encodes them. Both sides use duckdb-rs `=1.10504.0`, the version
nuthatch pins (the bench moved from 1.10501). **17/17 byte-identical**, including 325 KB of
`HUGEINT` arithmetic, signed zero, NaN, a 38-digit scaled decimal and an owl. Transcript:
`docs/bench/encode-parity.txt`.

**Errors** (`src/df/errors.rs`). DataFusion's text is restated in DuckDB's words, and the original
is kept below. That covers unknown table, unknown column (bare and qualified), syntax errors (the
engine now parses before its textual guard, so `SELEC 1` is a syntax error, not a refusal),
functions over `VARCHAR`, `bool_and(VARCHAR)`, `VARCHAR`/`BOOLEAN` mixes in `CASE` and `COALESCE`,
and out-of-memory. The kept text must not trip a class of its own ahead of the restated one, and
one did: DataFusion's own "No function matches" stole `COALESCE`'s class.
`burrmill-bench error-parity` runs each bad query on both engines through nuthatch's extractors,
and **11/14 land in the same class with the same extracted name**. The other three are not
wording, and each names its owner:

- quoted identifiers are case-insensitive in DuckDB (6.5);
- DuckDB casts `VARCHAR` to `BOOLEAN` implicitly (6.5);
- `SELECT from FROM t` is a syntax error to DuckDB, but sqlparser reads `from` as a table name (6.7).

A vanished segment already carries the path and the OS's wording (tested).

**Not done, and why.** The census's `checks/expected/*.json` no longer exist in nuthatch. The
encoder is proven against nuthatch's code on DuckDB's own batches. Byte identity at the engine
level also needs DataFusion to produce DuckDB's types (`/` as `DOUBLE`, microsecond timestamps),
which is 6.5. Corrupt-segment errors are unmapped, since this engine reads footers at open, not at
query time.

---

## 6.6c — against DuckDB itself; the allocator setting is declined — 2026-09-24

The gate is Burrmill's own bar. What justifies the swap is the incumbent, so `df-fold` gained
`MODE=duck`: DuckDB embedded as nuthatch embeds it, same 8-thread budget, batches dropped as they
arrive, and the same parity digest as every other mode. Thinkpad, one fold per process for RSS,
median of 5 for latency:

| | peak RSS | median |
|---|---:|---:|
| DuckDB 1.5 (`HUGEINT`) | 572-598 MB | 187-206 ms |
| **hosted Burrmill, streamed** | **240-262 MB** | **201-206 ms** |
| both with `MALLOC_TRIM_THRESHOLD_` | 474-480 / 215-223 MB | — / 254-264 ms |

**Same answer, same speed, 2.3x less memory, no tuning.**

**Decision: no allocator setting.** The trim threshold passes 256 MB, but its ~25% latency puts
Burrmill near 1.3x DuckDB, which fails gate 1's own "≤1.0x time-weighted". It is also no edge of
Burrmill's, because DuckDB benefits from it as well. The 256 MB gate stands as written, and the
last 5-20 MB are owed from inside the engine. Tokio's worker count is not where they are: 245-262
MB at 32, 8 and 2 workers.

Found on the way: **DuckDB's text-to-`DECIMAL(38,0)` cast is ~15x slower than to `HUGEINT`** on
this fold (1,440 ms against 92 on the MacBook, 11.1 s against 0.19 on the thinkpad). The first
`duck` run used `DECIMAL(38,0)` and made the incumbent look 55x slower; it now runs in its own
idiom. It bears on nuthatch as it stands: `_dec` is `TRY_CAST(... AS DECIMAL(38,0))`, so every
`SUM(x_dec)` in production pays that cast today.

---

## 6.6b — the thinkpad, and every RSS figure from today measured wrong first — 2026-09-24

**Correction first.** The 6.3, 6.6 and 6.6a memory figures below this entry came from 3-5 folds in
one process. This log already recorded (1.2) that allocator retention builds up across folds, and
that only one fold per process gives the operator's peak. I repeated that defect in a new harness.
`df-fold` now defaults to `REPEATS=1`. The latencies in those entries stand; the RSS columns do not.
Transcript: `docs/bench/df-fold-thinkpad-2026-09-24.txt`.

Same fixture (2M rows, 989,690 groups, 64 segments, 8 threads), one fold per process, four runs
each, commit `e2ff35a`. Latency is the median of 5, in separate runs:

| | thinkpad RSS | thinkpad ms | MacBook RSS | MacBook ms |
|---|---:|---:|---:|---:|
| stock DataFusion (wraps) | 491-525 | 200 | 534-545 | 100 |
| hosted fold, collected | 265-287 | 203-249 | 288-307 | 124 |
| **hosted fold, streamed** | **249-261** | **195-217** | 284-298 | 118 |
| bare fold (`fold`) | 208-227 | 157-161 | 269-280 | 97 |

**The hosted fold straddles the 256 MB gate on the thinkpad**: 249, 251, 260, 261 for `CAST`, and
250-257 for `TRY_CAST`. That is not a pass. It is 20-40 MB over the bare operator, and it is a third
of what stock DataFusion needs for the same answer.

Checked on the way:

- **The bare fold has not regressed.** Commit `fce2763` (5.5) built on the same box and interleaved
  with this one reads 200-220 MB against 208-225, at 155-158 ms against 157-161. That is noise, or
  at most under 10 MB.
- **Segment layout is not it**: one segment gives the same bare figure.
- **Tokio's 32 workers are not it**: 8 changes nothing.
- **glibc's arenas are real but not a remedy.** `MALLOC_ARENA_MAX=2` takes the hosted fold to
  289-305 MB and the bare one to 240-248 even with five folds retained, at 40-80% more latency.

**Most of the last 20-40 MB is memory glibc has freed and not returned.** Any explicit
`MALLOC_TRIM_THRESHOLD_` (128 KiB to 32 MiB, all alike) takes the hosted fold to **215-223 MB**, a
pass with over 30 MB spare, and the bare fold to 183-188. It costs latency: 254-264 ms against 205,
because setting the threshold at all switches off glibc's dynamic mmap threshold, so large buffers
are mapped and faulted in again on every fold. That is a setting for the host process, and the
library does not reach into the host's allocator (see 1.2a), so whether nuthatch sets it is a
nuthatch decision. Without it, the gate is straddled.

---

## 6.6a — streaming, and composite keys; the hosted fold within 20 MB of the bare one — 2026-09-24

- **`Engine::sql_for_each`** hands each batch to the caller and keeps none of them. The substituted
  fold now emits in 8,192-row chunks rather than one batch. Every sum is checked against 38 digits
  before the first chunk, so nothing downstream sees part of an answer and then a refusal. Its
  declared order holds across chunks (tested).
- **Keys built with `||` or `concat`** are recognised: literal tags, text casts (integers included)
  and `lower`/`upper`, as the curation views write them. A bare literal key such as `'sink'` counts
  too, which moved one 6.3 test onto the owned fold. It still refuses, now while reading the value,
  and the message no longer claims `TRY_CAST` was `CAST`.
- `df-fold` gains `STREAM=1`. The parity digest is now row-order, so a streamed digest and a
  collected one can be compared.

Same fixture, `STREAM=1`, MacBook, and the same digest from stock, 6.3 and 6.6 in both modes:

| | median | peak RSS |
|---|---:|---:|
| 6.6 collected | 124 ms | 381-404 MB |
| **6.6 streamed, `CAST` / `TRY_CAST`** | **118 / 119 ms** | **309-314 / 308-311 MB** |
| owned fold alone (`fold`) | 97 ms | 292 MB |

Hosting now costs about 20 MB and 21 ms over the bare operator. The gate is 256 MB, and the bare
fold measured 210 on the thinkpad, so the hosted path probably passes there. That is an inference
until it is measured on the thinkpad.

---

## 6.6 — the owned fold inside DataFusion plans; memory down a third, still over the gate — 2026-09-24

Moved ahead of 6.4/6.5 because 6.3 showed the gate failing on DataFusion's aggregate, and the
owned fold is the one operator here that has passed it.

`src/df/fold.rs`. An analyzer rule, ahead of `CheckedArithmetic`, swaps a matching `Aggregate` for
an `OwnedSignedFold` extension node; our physical planner lowers it to an operator that runs
`SignedFoldExec` in the engine's own pool. It shows in `EXPLAIN` at every stage.

**The rule may refuse where DataFusion answers, and must never answer differently.** So it only
substitutes when:

- the aggregate is one `SUM` of `[-]CAST|TRY_CAST(text AS DECIMAL(38,0))` over a `UNION ALL` of
  projections read straight off nest tables (nested unions, view pass-throughs and union coercion
  followed), keyed by columns or `lower`/`upper`;
- the value columns are non-nullable, or the sum is filtered by a NULL-rejecting comparison above;
  otherwise an all-NULL party would vanish instead of reading NULL;
- `TRY_CAST` runs strict, so a bad row refuses rather than dropping.

The output is checked against 38 digits, as the checked sum does. Where the predicate is exactly
`sum <> 0` the fold drops zeros itself, the filter selects every row, and arrow passes the batch
through uncopied. For a single key, the fold's canonical order is declared, and the sort above it
goes. Each substituted query is tested against the same statement made unsubstitutable
(`tests/df_fold.rs`).

Two defects surfaced on the way, and neither belonged to 6.6:

- **The owned fold read a NULL key as `""`** and merged the NULL party into the empty-string one
  (`[("", 12)]` for 5 and 7). Refused now, and committed separately.
- **EXPLAIN had only ever shown the plan as parsed**, because `MiniSession::optimize` lacked the
  umbrella crate's `Explain` handling. Ported.

**Same fixture as 6.3** (989,690 groups, 8 threads, 64 segments, MacBook, parity digest identical
across all modes):

| | median | peak RSS |
|---|---:|---:|
| stock DataFusion (wraps) | 100 ms | 589-593 MB |
| 6.3 checked, `CAST` / `TRY_CAST` | 107 / 102 ms | 606-647 / 731-745 MB |
| **6.6 owned fold, `CAST` / `TRY_CAST`** | **124 / 125 ms** | **381-388 / 395-404 MB** |

The fold itself takes 96-102 ms here, as it does standalone. The rest is building a million output
rows, which `collect` then holds (~73 MB of answer). So the gate still fails by about 1.5x on this
machine. The standalone fold (292-300 MB here, 210 on the thinkpad) excludes its answer by design;
this harness cannot. Hosting also costs about 24 ms over stock DataFusion on this fixture.

Owed: the thinkpad figures; a streaming `Engine` API so the gate measures the operator and not the
collected answer; composite keys with literal tags (the owned planner has them, this rule does not
yet); the `_dec` columns, which reach the fold only through `TRY_CAST` and fall back to 6.3's path.

---

## 6.3 — CheckedArithmetic lands; the memory gate fails, and not because of it — 2026-09-24

The 03 prototype, hardened, in `src/df/{wide,checked,rule}.rs`, as an analyzer rule between two
`TypeCoercion` passes. Every case in 03's table answers exactly or refuses, on the MacBook, in
`tests/df_checked.rs` (14 tests).

- **Per-type accumulator.** `Wide<N>` limbs: 2 for integers, 3 for Decimal128, 5 for Decimal256
  and text. Refusal stays order-independent: `MAX, +1, -1` answers.
- **Integer sums widen to `Decimal128(38,0)`**, as DuckDB's widen to HUGEINT. DataFusion's own
  `SUM(Int64)` and `SUM(UInt64)` wrap.
- **Whitelist.** Any function, aggregate or window producing an integer or decimal that is not on
  a named list is refused. So are shifts, and `+ - *` with one exact operand.
- **Literals** wider than u64 are refused at the surface, before they become Float64.
- **`TRY_CAST` is tracked as lossy.** Columns derived from one are tainted through projections,
  aliases, filters, joins and unions. `SUM`/`AVG` of one is rewritten to sum the source exactly:
  the rule threads the source up to the aggregate as a 320-bit value, negation included, so the
  signed fold over `TRY_CAST` (nuthatch's shape, and `_dec`) is exact. Any other aggregate over a
  tainted value is refused. DuckDB drops those rows.
- `checked_sum_text` is registered for exact uint256 totals as text.

One defect found by its own test: threading dropped the sign of a negated `TRY_CAST` branch, and
the signed fold answered +22 for -22.

**Memory, at 989,690 groups, 8 threads, 64 segments, MacBook** (`burrmill-bench df-fold`, one
mode per process, parity digest identical across all three):

| | median | peak RSS |
|---|---:|---:|
| stock DataFusion (wraps) | 100 ms | 589-593 MB |
| checked, `CAST` | 107 ms | 606-647 MB |
| checked, `TRY_CAST` (exact text sum) | 101-103 ms | 731-745 MB |

The checked cost is +17-57 MB on Decimal128, half of 03's +130 MB, and +145 MB on text, where the
state is 320-bit because a uint256 credit and its debit must be able to cancel. **The gate fails
by 2.3x before any of it**: stock DataFusion's two-phase aggregate is 590 MB here. A 192 MB
`FairSpillPool` with skip-partial reaches 339 / 360 MB at 2.2-2.8x the latency; smaller pools die
in the final sort. The owned fold reads 292 MB on this machine against 210 on the thinkpad, so the
absolute figures want repeating there.

Owed: the gate on the thinkpad; the authored views through the rule (the `df-views` harness
uses the umbrella crate, not `Engine`); the census function allowlist of 6.2.

---

## 6.1 / 6.2 — NestCatalog and lockdown, behind a feature flag — 2026-09-17

Phase 1 started on the C route. `burrmill` grows an optional `datafusion` feature:
component crates plus the owned `DefaultPhysicalPlanner`, concrete `Engine` API
(`sql` → `Vec<RecordBatch>`). Default `cargo tree -p burrmill` still has no
DataFusion.

`Engine::open_segments` / `open_nest` register nest tables from an explicit
file list. Declared-but-unsealed tables are empty. `word16`/`word32` columns
get a `_dec` companion. COPY, `CREATE EXTERNAL TABLE`, `read_csv`, and
`FROM '/etc/passwd'` are refused. Eight tests on the thinkpad, all green.
Default `cargo test -p burrmill` still green (edition bumped to 2024 for the
planner's let-chains).

Not in this slice: census function whitelist, `CheckedArithmetic`, JSON
encoder, dialect rewrites, `FoldSubstitution`.

---

## 6.0 — the component planner works, and it still fails burrmill#1 — 2026-09-17

Phase 0 of the swap, measured on the thinkpad, nuthatch's real profile
(`line-tables-only`, `CXXFLAGS=-g0`, six test binaries, thin LTO). Report at
`docs/research/replacing-duckdb/06-footprint-spike.md`.

C is a working engine. `DefaultPhysicalPlanner` ported out of the umbrella
crate (3,179 lines, `COPY` stubbed), component crates, concrete `net_balances`
API. Same fixture as DuckDB and the umbrella crate; all three print
`90 parties; first = (0x…0001, 3185)`.

| | duck | umbrella | components |
|---|---:|---:|---:|
| querying test binary | 162 MB | 493 MB | **446 MB** |
| non-querying test binary | 7.2 MB | 7.2 MB | 7.2 MB |
| cold `test --no-run` target | 3.27 GB | 4.48 GB | 4.14 GB |
| incremental after touch lib | 1.1 s | 1.9 s | 1.9 s |
| release binary | 41 MB | 127 MB | 105 MB |

C is 2.75× DuckDB on the binary that fills the disk, 2.6× on release. It is
8–17% smaller than the umbrella crate, and 87 s to compile against DuckDB's 77.
Investigation 05's "about even" was a lower bound that did not run the query
and used `debug = 0`. Instantiating the operators is what it omitted.

burrmill#1 fails. The plan's "otherwise U" fork is the worse of two failures;
if the swap proceeds it proceeds on C, as a trade signed in an RFC-0042
amendment, not as a footprint win. Phase 1 waits on that.

---

## Replacing DuckDB — the direction changes, and three claims are corrected first — 2026-09-16

Chief set the goal: swap DuckDB out of nuthatch for a Rust engine, with DataFusion or anything else
allowed under the hood. The bar moves from "≤1.0x DuckDB on every shape" to "no regression users
feel", and answer parity on every authored statement stays fixed. The work now has its own directory,
`docs/research/replacing-duckdb/`, and five investigations are running.

Before any of that, three things this repo said had to be corrected. Each was found by checking
rather than reading.

1. **The DuckDB wrap is not ours to report.** duckdb#24081 describes it, and #24168 fixed it on
   `main` six days after 1.5.5 was tagged. It is still in the 1.5.5 CLI: one thread errors, while
   two or four threads return `i128::MIN`. The upstream draft now says not to file, and roadmap
   2.3a says so too.
2. **4.2 cached footers on one side only.** DuckDB's `parquet_metadata_cache` is off by default and
   `views.rs` never set it. On the curation fold it measures 151 ms off and 138 ms on, which is
   enough to move the staking fold to about 1.0x. Filed as 4.2c. The README carries the caveat
   until a re-run.
3. **The README was behind the roadmap.** It still said "one plan shape" and "cold segments only"
   after stage 3 and 4.1a-d had landed. It now says 8/8 fold sub-plans and 0/65 statements, which
   is less flattering and true.

An external brief arrived the same day and is kept verbatim in the research directory, with its
corrections above it. Its central recommendation was to report the wrap upstream. The fix had
already merged by then.

### What the five investigations found

Each report is filed in `docs/research/replacing-duckdb/` with its corrections first. Two of the five
needed correcting before they could be filed.

- **01, what nuthatch needs from DuckDB.**
  - DuckDB is executor, parser (7 sites), RFC-0041 oracle, restart seed and admission bound.
  - The authored SQL is 132 statements, with the DuckDB-only syntax almost all in the Lodestar and
    qos views.
  - It also found a live nuthatch bug, confirmed here: the cold velocity seed is always empty
    (`docs/upstream/nuthatch-cold-velocity-seed-empty.md`).
- **02, DataFusion 55.**
  - The caches and extension points the design needs all exist.
  - Overflow is ours to own, and an unconfigured `SessionContext` will read `/etc/hosts` and write
    files.
  - None of the other Rust engines is a better host.
- **03, exact arithmetic.** A 320-bit accumulator plus an analyzer rule refuses or answers exactly
  through CTEs, joins and windows, at +100-130 MB at a million groups. The report said wide literals
  cannot be fixed; `parse_float_as_decimal` can fix them, untested.
- **04, the real views on DataFusion.**
  - 11 of 22 statements run with parity after three rewrites, at **0.71x DuckDB**, and 0.55x on the
    worst six with a provider that knows its files.
  - The agent's own figure was 0.60x. It had timed DuckDB through the parity path, which formats and
    sorts every cell, while DataFusion's time stopped at Arrow batches. DuckDB is now timed through
    `query_arrow` and both nests were re-run.
  - The README's 3.6x was DataFusion's defaults.
- **05, build footprint.**
  - The umbrella crate costs 2.6x bundled DuckDB per test binary at `-g0`, and 3x on the release
    binary, which fails burrmill#1. The component crates come out about even, but need an owned
    planner.
  - It also found `sqllogictest` in Burrmill's normal dependencies, beneath a comment saying it was
    a dev-dependency. Removed; `cargo test -p burrmill` passes.

**The plan** (`docs/research/replacing-duckdb/plan.md`, roadmap stage 6, RFC-0044 Amendment 2)
starts with a footprint spike. That decision should be made on a measurement before the
architecture sets around the umbrella crate by default.

---

## 4.2 — the headline claim, tested at last on the queries it is about — 2026-08-31

The README says Burrmill is "faster than DuckDB on the queries an indexer actually runs". Every
measurement behind that sentence had been a **synthetic** `net_balances` over a generated fixture.
The real authored views could not even be planned until 4.1a-d generalised the fold, and nobody went
back to check the claim once they could.

They can now, and the claim was false:

| fold | segments | DuckDB | Burrmill | ratio |
|---|---:|---:|---:|---:|
| `curation__signalled` + `burned` | 6,004 | 177 ms | 212 ms | **1.20** |
| four staking tables | 7,203 | 380 ms | 440 ms | **1.16** |
| curation + gns, four tables | 9,745 | 314 ms | 433 ms | **1.38** |

**0 of 3 at or under 1.0x**, on the same files, the same eight threads, parity verified. On the
synthetic fixture the same engine is 0.43-0.52x. Same code, opposite verdict, and the difference is
entirely the shape of the data.

### Why, and it was sitting in the metrics

A synthetic fixture is fifty segments and parses their footers in under a millisecond. A real nest
table is **six to ten thousand** tiny segments, and 57 to 93 ms of every query went into re-reading
them — a fifth of the whole query, paid again on every single request.

A sealed segment is content-addressed and immutable. That is already the stated reason the fold uses
`new_with_metadata` instead of re-parsing per morsel; the footers simply cannot go stale. So the cut
morsel list is cached on the segment set, shared across clones, and reset by `with_prefix`,
`from_files` and `refresh` because those change the file set.

`Morsel::path` became an `Arc<Path>` in the same change: a query clones that list once per fold, and
at ten thousand morsels a `PathBuf` there is ten thousand allocations for a string that never
changes.

| fold | before | after |
|---|---:|---:|
| curation | 1.20x | **0.80x** |
| staking | 1.16x | **0.95x** |
| curation + gns | 1.38x | **1.01x** |

Two of three now pass and the third is a rounding error away. The synthetic gate is untouched at
209 MB and 195 ms.

### What this says about the last two months of measurement

Everything in `docs/bench` was measured on a fixture whose *segment count* was nest-shaped only in
its size distribution, never in its cardinality. Roadmap 1.1a widened the fixture's **schema** after
it hid a missing projection pushdown for a whole slice; this is the same lesson about a different
axis, and it hid a 27% regression on the only queries anybody actually runs.

The synthetic gate is still worth having — it is reproducible and it isolates variables. But it is
not evidence about the product, and `burrmill-bench views` is now the thing to run before believing
any claim with "actually" in it.

---

## 5.1 — the cancellation contract, and the gate that had quietly broken it — 2026-08-31

RFC-0044 §3.5 makes a specific promise: **the delay between asking a query to stop and it stopping is
bounded by one morsel.** It contrasts this with DataFusion, whose joins do not yield to cancellation
at all (#19358). It is one of the reasons given for owning an operator rather than renting one.

It had **never been tested**. There was no cancellation test in the repository.

### Half of it was true

A query already folding stops promptly: the check sits at the top of `fold_morsel` and does what it
says.

### The other half had been broken three items earlier, by me

Roadmap 5.3 put an admission gate in front of the pool to fix a starvation bug. A yield point *inside*
the fold says nothing about a query that has not reached the fold — and with the gate occupied, a
cancelled newcomer sat in the queue until admitted and only then noticed.

Measured: **57 ms to notice, against a whole query of 110 ms.** "Bounded by one morsel" had become
"bounded by however long everybody ahead of you takes", silently, in a commit whose own tests all
passed.

The gate is a yield point now: a timed wait with a two-millisecond notice, well inside a morsel, paid
only by threads that are already blocked. Serving throughput and single-query latency are both
unchanged — 136/136/117 qps at 4/16/32 clients against 133-143/127-135/115-124, and 173-195 ms
against a band of 185-199.

### The subtle half of the fix

Admission is a ticket queue, so a waiter that gives up must **forfeit** its turn rather than merely
walk away. A ticket taken and never completed holds the line up for everyone behind it: the gate
would lose one slot per cancellation and seize after `width` of them. That has its own test, and the
test would *hang* rather than fail if the forfeit were wrong — which is why the comment says so
beside it.

### The lesson, which is the day's most-repeated one in yet another costume

A fix for one property broke another, and nothing caught it because **the broken property had no
test**. Not "the test was weak" — there was no test at all, for a guarantee written into the RFC and
into the crate's own documentation. Three sessions of work sat on top of it.

The first version of the test also passed while proving nothing: the query hogging the gate ran in a
loop, so it released its turn between iterations and let the queued query straight in. A test that
cannot fail is not evidence, and it took making it adversarial before the bug appeared.

---

## 5.5 — it did not need a scheduler, it needed the constant tuned at the right size — 2026-08-31

5.5 was filed as "work-stealing across queries; a scheduler, not a constant". Before costing that,
the question worth asking was why a *single* query only gets 3.3x from eight threads.

**It depends on query size.** A 181 ms fold scales 7.9x. A 48 ms one scales 4x. Not per-segment cost
— the speedup is the same with one segment as with two hundred — and not decode.

It is **lock contention** on the aggregate's sixty-four partitions. Each flush takes up to sixty-four
locks, so a worker that flushes proportionally more often contends proportionally more, and a small
query flushes more often relative to its total work. `FLUSH_ROWS` 4,096 → 16,384 takes a 500k-row
scan from **18 ms to 11** at eight threads while the single-threaded cost does not move at all —
parallel time changes, serial time does not, which is contention and nothing else.

### Why the constant was wrong

It was swept in roadmap 1.2, honestly and with a table, at **a million groups on thirty-two
threads**. At that operating point memory binds: every worker holds a scatter buffer, so the constant
multiplies by the fold's width, and 4,096 won on peak RSS.

A serving query is small, runs at eight threads, and is nowhere near the memory budget. There the
same constant is a *contention* knob rather than a memory one, and the answer is four times larger.
The sweep was not wrong; it answered a question about the wrong workload, and I never went back to
ask whether the workload had changed underneath it.

That is worth naming, because it is the day's most repeatable mistake in a new form: a number
measured properly, at an operating point that later stopped being the one that mattered.

### What it bought

| clients | before | after |
|---:|---:|---:|
| 4 | 123 qps | **133-143** |
| 16 | 123 qps | **127-135** |
| 32 | 112 qps | **115-124** |

Tails better, fairness unchanged at ~0.90, and the memory gate holds at **218 MB** with ratios
0.43-0.50 and parity verified. Six megabytes for eight to sixteen per cent.

### And a reminder that a busy machine lies

The first run of this measured the 1M-group fold at 653 ms against 196, and serving at 41 qps — with
**DuckDB collapsing by the same factor**, which is what gave it away. A DuckDB rebuild was running:
load average 34, five `cc1plus` at 100%. Every number in that block was void. Two engines degrading
together is a machine, not a change; had only one moved I might have believed it.

---

## 5.4 — the throughput gap was utilisation, not work — 2026-08-31

The obvious suspects were per-query fixed cost: footers parsed afresh on every query, no plan cache,
no metadata cache. All wrong, and the measurement that settled it took two minutes.

**The fold is 53 ms at one thread.** DuckDB's implied serial cost at 168 qps on eight threads is
~48 ms. The work is comparable; we are not doing more of it. What we were doing was using eight
threads to get **3.3x** — scaling saturates at four and the eighth thread buys nothing.

Under load that compounds. A sharing query was split into exactly `d` coarse groups, so two groups of
250k rows each left a worker idle for however much the slower one ran over by. Eight threads were
about two thirds busy against a work-bound of ~151 qps.

Splitting each admitted share **twice** recovers most of it. Swept, two runs each, 32 clients:

| groups per share | qps | worst p99 | fair |
|---|---:|---:|---:|
| d × 1 | 101, 104 | 432, 451 ms | 0.89, 0.94 |
| **d × 2** | **112, 113** | **446, 402 ms** | **0.95** |
| d × 3 | 89, 91 | 515, 526 ms | 0.81 |

A sharp optimum rather than a trend: one group per worker balances badly, three brings the per-group
fixed cost back. Two is a constant in the code and not a setting, because a library that claims
nothing to configure should not grow a knob the moment a number is inconvenient.

Over-decomposing is only safe **because admission bounds concurrency now**. Before the gate, it was
precisely what let one query take the whole pool and starve the queue. The same change would have
made things worse two items ago.

### Where the serving picture stands

| clients | duck_multi | burrmill |
|---:|---|---|
| 1 | 56 qps | **77 qps** |
| 4 | 109 qps, fair 0.88 | **123 qps**, fair 0.85 |
| 16 | **149 qps**, fair 0.49 | 123 qps, **fair 0.93** |
| 32 | **167 qps**, fair 0.57 | 112 qps, **fair 0.89** |

Burrmill wins outright to four clients, is markedly fairer at every count, and has a comparable tail
(410 ms against 400 at thirty-two). It trails on raw throughput above eight clients.

Single query unchanged: `181 188 189 189 190` ms against a baseline band of 185-199.

### What is left, costed rather than guessed

112 qps against a work-bound of ~151 is about 74% utilisation. DuckDB reaches 168, which is *above*
our work-bound, so it is both roughly 10% cheaper per query and better utilised. Closing that means
work-stealing **across** queries instead of nested rayon pools — a scheduler, not a tuning constant —
and it is filed as 5.5 to be costed before anyone starts it.

---

## 5.3a — a sharing query takes a slice of the pool, not all of it — 2026-08-31

The gate fixed *how many* queries start. It did nothing about how **wide** each one got, so four
admitted queries still fought over eight workers and throughput stayed where 5.2 left it. A query
admitted while others are queued now splits into exactly `pool / in_flight` groups, which caps its
parallel width because rayon cannot run more groups at once than there are.

At 32 clients on the 32-core box, against 5.3:

| | 5.2 | 5.3 (gate) | 5.3a (+degree) |
|---|---:|---:|---:|
| throughput | 96 qps | 89 | **100** |
| worst-client p99 | 2378 ms | 601 | **478** |
| fairness | 0.00, starved | 0.81 | **0.94** |

At four clients Burrmill now **beats** `duck_multi` outright — 108 qps against 107 — and it is fairer
at every count: 0.94 against 0.72 at thirty-two. Liveness, tail and fairness are all now at or better
than DuckDB's.

### The bug in the first version, which the benchmark found and the code could not show

"Alone" was inferred: a degree equal to the pool size meant nobody else was waiting. That comparison
was made against `rayon::current_num_threads()` inside the pool, which is not necessarily the number
the caller divided by — and when the two disagreed, a solo query was treated as *sharing* and capped
to eight groups instead of over-decomposed four-ways-per-thread for balance.

The cost was invisible in the median and plain in the tail: single-query samples went from
`185 188 189 190 191 199` to `187 189 195 198 242 266`. Nothing in the code looked wrong, because
nothing in the code *was* wrong except an assumption about what a library function returns.

The caller now says `None` for "I have the pool to myself" rather than leaving the executor to work
it out. Single query back to `185 191 191 192 192 197` against a baseline band of 185-199.

**A/B against the one changed line is what found it.** Removing `.with_degree(...)` and rebuilding
took two minutes and turned "probably noise, the code path is identical" into a measurement. It was
not noise, and the code path was not identical.

### The standing gap, stated plainly

Burrmill is **100 qps at 32 clients against DuckDB's 168** — about 0.6x — and that is now the honest
remaining weakness. It is no longer a fairness or liveness problem; it is raw throughput under load
and it is undiagnosed. The obvious suspects are per-query fixed cost (footers are parsed afresh on
every query, and there is no plan or metadata cache at all) and a narrow query still paying the full
morsel-scheduling setup. Filed as 5.4 rather than guessed at.

---

## 5.3 — the starvation was a liveness bug, and it is gone — 2026-08-31

5.2 reported clients being "starved". Before building anything I checked whether that was my harness
rather than the engine, because each client makes an untimed warm-up call and one still inside it
when the window closes reports zero queries and reads as starved.

It was not the harness, and the check made the finding much sharper. **At a twenty-second window, one
client completed 220 queries and another completed none** — it spent the entire twenty seconds inside
its first query. That is not a slow queue. A query that never runs is worse than a query that is
refused, because the caller has nothing to act on.

### Why, given there is no lock

Rayon's workers prefer their own local deques and look at the injector — where every queued query
waits — only when those run dry. A query in flight keeps eight workers generating subtasks for each
other, so under continuous load the injector can go unvisited indefinitely. **A bounded pool with no
fairness starves the queue exactly as a mutex does**, and the absence of a mutex says nothing
whatever about it. DuckDB behind its connection mutex fails the same way and for the same reason;
we had simply assumed the mutex was the mechanism rather than one instance of it.

### The gate

A ticket lock with a width, in front of the pool. A query takes a ticket and proceeds when
`ticket < released + width`, so admission is first-come-first-served and at most `width` queries are
inside at once. Nothing clever — the point is that the ordering is **ours** and therefore knowable,
rather than an emergent property of a work-stealing scheduler that was never asked to be fair.

At 32 clients on the 32-core box:

| | before | after |
|---|---:|---:|
| fairness | 0.00, some client served nothing | **0.81, everybody served** |
| worst-client p99 | 2378 ms | **601 ms** |
| throughput | 96 qps | 89 qps |

**Liveness restored and the tail cut fourfold, for eight per cent of throughput.** A panicking query
releases its turn on unwind, tested, because one bad query wedging the serving path would be a worse
bug than the one being fixed.

### What the width is not

Sweeping admission width from 1 to 32 moves throughput only **72 to 98 qps** while the worst tail
goes 428 ms to 3023 ms and fairness collapses to zero. So width is a fairness knob, not a throughput
one, and the default of half the pool sits at the knee. Choosing it by measurement rather than by
taste is the only reason to trust it.

### What remains, stated precisely

Burrmill is **89 qps at 32 clients against `duck_multi`'s 170**, and admission cannot close that: the
whole width sweep tops out at 98. DuckDB stops parallelising a query when there are others waiting;
we split every query across the whole pool regardless of how many are queued behind it. The fix is a
degree on the fold — `coalesce` producing exactly `d` groups with `d = pool / in_flight` — which caps
a query's *width* rather than merely limiting how many start. Filed as 5.3a rather than done, because
it touches the hot path and the gate it would be built on has only just been measured.

---

## 5.2 — the concurrency claim did not survive being measured — 2026-08-31

RFC-0044 §7 calls this "the easiest headline in the project", on the grounds that DuckDB sits behind
one connection mutex and Burrmill takes no global lock. Half of that is right and the conclusion is
wrong.

### What reproduces

DuckDB embedded the way nuthatch embeds it — one `Connection` behind a `Mutex` — behaves exactly as
#986 said. On a 32-core box: **55 qps at one client, 49 at thirty-two**, flat, and at every count
above one some client is served **not at all**. The mutex finding is real.

### What does not

Embedded its own way — one database, a connection per client — DuckDB scales.

| clients | duck_shared | duck_multi | burrmill |
|---:|---:|---:|---:|
| 1 | 55 qps | 56 qps | **80 qps** |
| 4 | 54 | 109 | 99 |
| 16 | 53 | **155** | 103 |
| 32 | 49 | **171** | 96 |

Worst-client p99 at 32 clients: DuckDB **423 ms**, Burrmill **2378 ms**. Fairness: DuckDB serves
every client, Burrmill serves some of them nothing at all.

**Burrmill is the faster engine at one client and the slower one at sixteen.**

### Why, and it is not the lock

There is no lock. There is a **bounded shared pool**, and a pool that hands all eight workers to one
query at a time starves the queue exactly as a mutex does. The thread budget makes it plain — 32
clients, 32-core box:

| threads per query | qps | fairness |
|---:|---:|---:|
| 1 | 15 | 0.67, everybody served |
| 2 | 32 | starved |
| 4 | 55 | starved |
| 8 | 99 | starved |
| 16 | 129 | starved |

One thread per query serves everyone at 15 qps. Eight threads serve some clients nothing at 99. And
the arithmetic between those rows is the finding: **eight independent single-threaded queries would
be about 120 qps by that scaling, where eight-way parallelising one query at a time gives 99.**
Parallelism per query is not free under load, and Burrmill spends it regardless of how many clients
are waiting. DuckDB stops parallelising each query when there are others to run; we do not.

Filed as 5.3: a fair queue in front of the pool, and per-query parallelism that shrinks as load
rises. Note that no constant fixes this — `max_threads` picks between "slow and fair" and "faster and
starving", and neither is a serving profile.

### The measurement lied first, as usual

The first version reported **82 qps at 32 clients with a 14 ms p99**, which cannot both be true: 32
clients sharing one mutex at 12 ms a query is not 82 qps. Pooling every client's latencies weights by
throughput, so a starved client contributes almost no samples and cannot move a percentile however
badly it was treated. The per-client counts said what was happening: one client ran 191 queries and
another ran **none**.

`worstp99` is now the worst client's own p99 and `fair` is the slowest client's share of the
fastest's. That is the seventh measurement fault in this project's short life, and the second where
the naive metric was flattering in *both* directions at once.

There was also a strawman in DuckDB's favour, caught on the first run: `Connection::open_in_memory()`
per client is a fresh **database** per client, so four clients meant four independent DuckDB
instances with eight threads each. It read as DuckDB scaling beautifully at 221 qps against eight
threads' worth of work, which is what gave it away.

### Why this was the right order

5.1 is streaming results and the async cancellation contract, and it would have been built directly
on top of the assumption this just falsified. Measuring the claim before building on it is the whole
of the argument for doing 5.2 first, and it is the second time today that running the experiment
first saved building the wrong thing.

---

## 4.1d — every fold in the workload now plans and runs — 2026-08-31

**Fold sub-plans: 8 of 8.** The last one carries two aggregates over a composite key, which the real
delegation view writes to keep tokens and shares together.

Unlike the composite key, this one genuinely changes the aggregate, so the constraint was that a
single-`SUM` fold must not pay for it. Aggregates past the first live in a side map keyed by
`(arena offset, index)` — the same shape as the `wide` overflow map beside it — so `Entry` keeps its
one inline `i128` and the hot loop is untouched. Widening `Entry` to carry m sums would have put
sixteen bytes per group on every query in order to serve one of them.

The canonical sort has to carry the extras: they are addressed by row position, so sorting the index
alone would leave every row past the first sum pointing at somebody else's number — a wrong balance
with no error at all. Sorting a permutation and applying it is what that costs, and only the
multi-`SUM` case pays.

`HAVING` is refused when there is more than one `SUM`, because "drop the rows that net out" has to
say *which* sum, and discarding a row whose other sum is non-zero would throw away a real answer.

### It was not free twice, and the benchmark said so both times

**A one-element loop is not the same as no loop.** Putting every arm through `for (j, vi) in vis` with
a bounds-checked `b.values[j]` per row cost **15%** — 100 ms to 116. The one-sum, one-column arm now
has its own list, built once per batch.

**`Pending` grew from 32 bytes to 48.** Adding a bare `u16` for the aggregate index next to an `i128`
costs sixteen bytes to alignment, and every worker's scatter buffer with it. It is packed into the
spare high bits of the length instead: keys are addresses, and sixteen bits of length is 65,535 bytes
of one.

After both: 199 MB and 178 ms on the canonical box against 208 MB and 177 ms before — inside the
previous spread, parity verified, both corpora green.

That is three times in this stage that new machinery was not free until it was measured. The rule is
easy to state and apparently hard to obey: **a feature nobody used still has to cost nothing.**

### Where the coverage ratio actually stands

Every fold-shaped sub-plan in a 65-statement real workload now plans and executes. **Statement-level
coverage is still 0 of 65**, because all eight sit inside a `WITH` binding or a join, and that is the
honest number to publish beside the other one. Owning the fold is the point — it is the heavy part —
but §4.6's published ratio does not move until CTEs and joins are admitted, which is 4.1f.

---

## 4.1c — composite keys, and a decision about what *not* to build — 2026-08-31

Fold sub-plans **6 of 8 to 7 of 8**. The two remaining folds looked like one job and are two, which is
the whole content of this item.

- `SELECT curator, position, SUM(sig) ... GROUP BY 1, 2` needs a **composite key**. The aggregate
  never learns about it: the executor builds one byte string and the table hashes bytes.
- `SELECT delegator, sp, SUM(tok), SUM(sh) ... GROUP BY 1, 2` needs **two aggregates**. `Entry`
  carries one `i128` and would need m, which is a different operator and a real regression risk to a
  gate that only just started passing.

So the cheap, safe one is done and the expensive one is filed as 4.1d rather than rushed at the end
of a long day.

A key column is now a bare column, a string literal, `lower`/`upper` of a column, a cast of one to
text, or any `||` concatenation of those. The literal is not decoration: the real curation view tags
its key `'v:' || id` against `'n:' || id`, and without the tag a subgraph's *version* signal and its
*name* signal - different ids in different namespaces - are added together as one position. A test
asserts both halves of that: tagged gives two positions of 100 and 5, untagged gives one of 105.

**Length-prefixed, not delimiter-joined.** `("ab","c")` and `("a","bc")` are different keys, and any
separator byte turns up in data somebody has not shown me yet. Four bytes of length per column beats
a delimiter and a hope.

### Two things the tests caught, both the same mistake

**The prefix condition did not match the un-prefix condition.** The executor prefixed whenever it
left the fast path; `Rows` un-prefixed only when the arity was above one. A `lower(addr)` key is
arity one *and* off the fast path, so the caller got `"*\0\0\00xabcdef..."` with a correct sum
attached. Two halves of one decision must share one condition.

**And the fast path stopped being fast.** Deciding per row whether an arm was simple, by reaching
through a `Vec<Vec<Option<StringArray>>>`, cost **20%** of the fold - 100 ms to 120. The split is now
made once per batch, so an arm with one bare column keeps a `StringArray` in hand and a loop with
nothing in it but the push. Back to 101-103 ms, and 208 MB on the canonical box with parity verified.

New machinery has to be free when it is not used. It was not, and only the benchmark said so.

---

## 4.1b — the subset was not too narrow so much as wrong about SQL — 2026-08-31

Fold sub-plans admitted go **1 of 8 to 6 of 8**. The interesting part is why, because I expected one
cause and there were three, and two of them were Burrmill being wrong rather than strict.

**`lower()` on the group key** (4 folds). The real ERC-20 balance view is
`SELECT lower("to") AS addr, ... UNION ALL SELECT lower("from")`, because an address differing only
in case is the same address and a fold that treats them as two parties reports two half balances with
no error at all. Admitted as a deliberately tiny allowlist — `lower` and `upper`, one bare column —
rather than an expression evaluator, because admitting arbitrary expressions is admitting a language.
The list grows one measured entry at a time.

**An alias demanded where SQL does not name one** (1 fold). A union takes its column names from its
first arm; later arms are positional. `SELECT indexer, -CAST(tokens AS HUGEINT) FROM ...` is perfectly
well formed as a second arm, and Burrmill refused it over a rule SQL does not have.

**Later arms checked against the first** (1 fold). Worse than the last one, because I *added* that
check in 4.1a. A five-table staking fold reads `indexer AS sp` in its first arm and `"serviceProvider"`
in its third — two different columns holding the same thing, which is precisely what a union is for.
Even an explicit alias on a later arm is ignored by every engine, so the check is gone.

Both of the last two were **my rules, not the RFC's**, and both were refusing SQL that every other
engine accepts. "The admitted subset is small" is a design decision; "the admitted subset is wrong
about the language" is a bug, and it is worth keeping the two apart.

`lower()` and the positional-arm shape are tested by **execution**, not by planning: a test asserts
that three spellings of one address fold to one party with the right total, and that without `lower()`
the same data is three parties. The difference is a fact rather than a claim.

No regression: 100-104 ms and 245 MB at a million groups, which is the pre-generalisation baseline to
the millisecond.

### A correction to yesterday's A4 headline

I wrote that the one-table-read-twice shape "occurs **zero** times in the workload it was built for".
That was wrong. My detector counted union arms without checking whether the arms read *different*
tables, so a two-arm fold over one table was filed as multi-table.

Measured properly: of the 8 folds, **4 read one table** — the ERC-20 `Transfer` shape, where one row
carries both a payer and a payee — and 4 read several. The one-table shape was being refused over a
`lower()` call, not over its shape.

The substance survives: the multi-table form was entirely unsupported and is 4 of 8. But "the operator
was built for a shape that does not exist" was too strong, and the tool now reports the split so the
claim cannot drift again. Sixth measurement fault of the day, and the second I published before
catching.

### What is left

Two folds, both the same thing: a **composite group key with several aggregates** —
`SELECT delegator, sp, SUM(tok), SUM(sh) ... GROUP BY 1, 2`. That is a genuinely different operator,
not another relaxation, and it is filed as 4.1c.

---

## 4.1a — the fold is n-branch now, and the shape it was built for was the wrong one — 2026-08-31

A4 found that Burrmill folded *one table read twice* and that no real query does. `SignedFold` now
carries `Vec<FoldBranch>` — table, key column, value column, sign, cast mode — and the old shape is
the degenerate case of the same table listed twice with opposite signs. Nothing was lost, which is
usually the sign that the general form was right all along.

Three changes came with it, each because the workload asked for it rather than because it was tidy:

- **n arms, not two.** A4 found folds with four and five arms, so the old limit was a statement about
  the implementation rather than about the shape.
- **`CAST` as well as `TRY_CAST`, kept distinct.** Every real fold is written with plain `CAST`. The
  old rule refused it to protect a semantic nobody had asked for. They mean different things —
  `TRY_CAST` skips a bad value, `CAST` errors — so the plan carries which was written and the
  executor honours it. Reading one as the other would change an answer silently.
- **`GROUP BY 1`.** What the views are written with, unambiguous over a two-column projection.
  Refusing it was refusing a spelling rather than a shape.

### The performance trap, and it was real

The degenerate shape becomes two arms over one table, and the naive reading is two passes over files
one pass already has in hand. So branches are grouped by table and a table is scanned once however
many arms name it.

That was not enough on its own. The benchmark went **104 ms to 120** anyway, because both arms then
parsed the *same* forty-digit value text independently, once each per row. Parsing per distinct value
column instead of per arm put it back: 99-116 ms, inside run-to-run spread. Peak RSS at a million
groups is 210 MB against 210 before, the ratios are 0.40-0.90, and parity is verified throughout.

Trading measured throughput for coverage is a fair trade; doing it without noticing is not.

### What it actually bought

**Fold sub-plans admitted: 1 of 8, up from 0.** Stated at the sub-plan level because every one of
those folds sits inside a `WITH` binding or a join, so statement-level coverage stays 0/65 until CTEs
and joins are admitted — which is a different item, now filed as 4.1d.

Modest, and the remaining barriers are counted rather than guessed:

| still refused | count |
|---|---:|
| the group key is an expression, not a bare column | 4 |
| the outer projection is wider than key + sum | 2 |
| a computed projection with no explicit alias | 1 |

Computed group keys are the next lever: real views write `'v:' || CAST("subgraphDeploymentID" AS
VARCHAR) AS position`.

### And a self-inflicted one worth recording

Rewriting the executor's row loop, I gave a Python edit a start anchor after its end anchor. Python
slices that to an empty string, `replace("")` matches between every character, and a 534-line file
became **1,084,412 lines**. Restored from HEAD and redone with unique, asserted anchors. *Query: why
did I think an unchecked `s.index()` pair was a safe way to edit a file?*

---

## Experiment A4 — the operator is built for a shape the workload does not contain — 2026-08-31

§4.6's coverage ratio is `owned shapes / n`, and the whole "hybrid now, own more later" argument
turns on whether `n` is a dozen or unbounded. It has been open since the RFC was written. It is
answerable, and the answer was sitting in the authored views of every nest on this machine.

126 view files, 65 statements, every one parsed and handed to **the real planner** rather than to a
model of the admitted subset that could drift from it.

### The count

**32 distinct shapes. 22 plan families.** Two numbers because the question means two things, and
publishing only the flattering one would have been the day's fifth measurement fault. A shape is the
exact feature set; a family collapses the aggregate mix, because a grouped aggregate carrying k
accumulators is one operator parameterised by k rather than k operators.

Neither is a dozen. The top five families cover 48% and the tail is long. But the 22 families are
compositions of just **nine primitives** — cte, set-op, join, subquery, window, distinct, group-by,
having, agg — which argues for owning operators that compose rather than enumerating plan patterns.
That is what §4.3 already says; what A4 adds is that the planner cannot be a lookup table of shapes.

### The finding that matters more than the count

**Coverage ratio: 0 of 65.**

Burrmill's admitted shape is "one table read twice, one column crediting and one debiting the same
signed value". Every single `UNION ALL` in the entire workload reads **different** tables:

```sql
SELECT dep, SUM(tok) FROM (
  SELECT "subgraphDeploymentID" AS dep,  CAST(tokens AS HUGEINT) AS tok FROM curation__signalled
  UNION ALL
  SELECT "subgraphDeploymentID" AS dep, -CAST(tokens AS HUGEINT) AS tok FROM curation__burned
) GROUP BY 1
```

Which is obvious in hindsight and was not obvious in advance: **a credit and a debit are different
events, so they are different tables.** A single table carrying both a payer and a payee column is
the ERC-20 `Transfer` shape, and the authored views do not fold one.

**8 of 65 statements are n-table signed folds** — five with two branches, two with four, one with
five. The one-table case Burrmill owns occurs **zero** times. The benchmark query came from the #987
spike rather than from the workload, and nobody checked.

Generalising `SignedFold` from one table to n branches takes coverage from 0% to about 12% and throws
nothing away: the current shape is the degenerate case of the general one, and the machinery for two
pipelines into one aggregate was built last item for the seam.

After that, **projection width is the next lever**: 41 of 65 refusals are "projects exactly the key
and the sum; got N items", with N from 3 to 13.

### The measurement nearly lied, again

The first version of the fold detector reported **0 of 65** n-table folds. It looked only at the
top-level query body, and the fold above lives inside a `WITH` binding. Publishing 0 would have been
wrong and rather damaging.

It was caught by having read one of the files first and disbelieving the tool when it disagreed. That
is the fifth measurement fault in a day and the first one caught *before* anything was published,
which is at least the right direction. **A measurement that finds nothing is the one to distrust
hardest.**

---

## Stage 3 — the seam, and the three bugs COR-1 found — 2026-08-31

The highest-risk invariant in the design, M/Critical in the risks table. It holds, and getting there
cost three real bugs — two of them mine, one nuthatch's.

### The RFC's summary of the seam was not what the code does

§3.4 describes "a redb hot tip ∪ sealed cold Parquet, modelled as a disjoint `UNION ALL` over a
single monotonic boundary". Reading nuthatch's `store.rs` and `seal.rs` instead of the precis:

- The redb store is `entities` / `meta` / `blocks` / `outbox`, keyed strings holding JSON. Its own
  header says it is "the tip layer for **entity point-reads**".
- Rows leave hot when their block range is final. All tables seal together per range, so
  **`sealed_through` is one global watermark**, not per-table.
- `prune_and_set_meta` advances the watermark and prunes hot **in one transaction**.
- Cold is append-only and never sees a reorg; reorgs only touch hot.

That last pair is the whole invariant, and it settles a question Chief raised in passing: **the
choice of hot store is not load-bearing.** COR-1 rests on the store offering snapshot isolation,
which redb does and Turso would too. RFC-0044 §9 already lists "No Turso/redb replacement" as a
non-goal, and the mechanism agrees with the non-goal.

### The ordering, which is the whole thing

Pin `S`. Cold is `block <= S`, hot is `block > S`, and both must come from a view where those are
true at the same instant. The dangerous order is: read the watermark, then read hot — a seal landing
in between moves a range into a segment nobody listed and out of the hot rows that were read. The
range is in **neither half**, and a fold that drops rows returns a short balance, which looks exactly
like a balance.

So `HotTip::snapshot` hands back the watermark and the rows **together**. A two-call interface is the
bug's natural shape, and the trait is built so it cannot be written rather than so it can be
documented.

### Bug one: the ordering constraint spanned the catalog, and the API let a caller break it

`query()` took the hot snapshot but used the catalog the caller had already built — so the cold
listing was from whenever the handle was opened. COR-1 caught it on run 0.

A caller cannot be expected to open its handle at the right instant; it has no way to know when that
is. `SealedSegments` now remembers where it was discovered and the seam re-lists it **after** pinning
the snapshot. Sealing is append-only, so a later listing is a superset of what the watermark
promises.

### Bug two: `sealed_through: u64` had a sentinel that could not mean both things

Zero has to mean "nothing is sealed" and "block zero is sealed", and it cannot. The test found it on
a genesis-block row within five runs. It is `Option<u64>` now — a sentinel in a boundary is exactly
where seam bugs live, so the ambiguity is removed rather than special-cased.

### Bug three, and it is nuthatch's: segments are installed non-atomically

`seal.rs:176` is `std::fs::write(seg_dir.join(&file), &bytes)`. That creates the file and then writes
it, so a reader globbing `segments/` sees a zero-length Parquet. The test reproduced it faithfully
and failed with *"Parquet file too small. Size is 0 but need 8"*. The manifest ninety lines further
down the same file **is** installed tmp-then-rename, with a comment explaining precisely why.

Burrmill cannot work around this. Given a segment it cannot parse it can skip it — correct if the
file was mid-write, and a silently short balance if it was corrupt — or refuse, which is correct for
corruption and fails every query issued during a seal. It cannot tell them apart, so it refuses and
says so, naming the file and pointing at the write. The fix belongs at the writer, where the
distinction is free. Drafted at `docs/upstream/nuthatch-segment-install-not-atomic.md`, not sent.

### The test

A conserved quantity: every row credits one party and debits another by the same amount, so the
per-party answer is fixed by the data and does not depend on where the boundary sits. Both failure
arms show up in that one number — a double-count inflates two balances, a drop deflates two — and
**neither would raise an error on its own.**

**167 to 250 folds per run, every one of them overlapping an active sealer**, stable across four
runs. The test counts the overlap and fails if it is small, because a COR-1 test that folds a settled
nest passes just as happily with the invariant broken.

### What is deliberately not built

**No redb dependency.** The hot rows live there as JSON entities in a schema that is nuthatch's
business, and 1.4 is the standing lesson about guessing at nuthatch's layout. What is owned here is
the invariant; a redb-backed `HotTip` is a thin adapter and belongs where that encoding is known.

---

## The four open decisions, made — and slice 1's gate passes — 2026-08-31

Chief handed over the four decisions with the instruction to call them for performance and safety.
They are not independent, and that is what decides them: bounding parallelism is what buys the
memory to make the refusal order-independent.

### 1.2c — the gate applies at eight threads per query, and the operator enforces it

`Limits::max_threads`, default 8, with the handle owning a pool that size. RFC-0044's concurrency
argument is about thirty-two *clients*, not thirty-two threads for one of them; #986 measured DuckDB
going 40.3 to 39.6 qps between one client and thirty-two while p99 went 29.5 ms to 7066 ms, and a
fold that hands every core to a single query has reinvented that by another route.

The cores past eight are not buying anything either. At 1M groups on a 32-core box: 601 ms at 1
thread, 223 at 4, **171 at 8**, 156 at 16, 144 at 32. Eight is within 6% of the whole machine and
leaves twenty-four cores for other queries.

**And it is what makes `mem_pool_bytes` mean something.** The same binary measured 145 MB on one
thread and 340 on thirty-two. A budget that depends on the host's core count is not a budget.

### 2.1a — refuse, do not guess

Three options and only one is safe. Matching DuckDB means adopting `7.9 → 8` — silent rounding, into
an engine whose first claim is exactness — and implementing a numeric grammar by guess at every other
edge. Continuing to skip means a row DuckDB counts and we drop, which is a short balance that looks
entirely plausible.

So: a value that carries digits but is not a canonical integer is **refused, naming the value**.
`TRY_CAST`'s NULL still applies to data that is genuinely absent or non-numeric, which is what the
query asked for; it is text carrying a number that this will not interpret, because interpreting it
is guessing. Diverging from DuckDB out loud costs a query. Diverging silently costs someone's answer.

### 2.1b — yes, and it turned out to be free

Refusal now depends on the **answer**, not on whether some partial sum left the range. `MAX, +1, -1`
sums to exactly `MAX` and is returned; `MAX, +1` is refused whatever order it arrives in. A query
that succeeds on Tuesday and fails on Wednesday with the same data is not something a serving engine
can offer.

The feared cost was 16 bytes per group — ~28 MB at a million groups against a budget already being
missed. It costs **nothing**, because the high word lives in a side map keyed by arena offset and an
entry only acquires one once its running total actually leaves `i128`, which on real data is never.
`Entry` did not grow. Peak RSS at 1M groups: 210 MB before, 210 MB after.

The first implementation was wrong in an instructive way: promoting an entry to wide set its high
word to zero rather than sign-extending it, so a negative running sum was silently reinterpreted as
`2^128 - n`. The generated corpus caught it within a second of the test running. That is the entire
argument for having built it.

### 2.3a — drafted, not sent

Reporting the DuckDB wrap goes out under Chief's name, so it waits for him.
`docs/upstream/duckdb-hugeint-parallel-wrap.md` has the reproduction and the version.

### The harness was unfair again, in the same shape as this morning

The moment the thread bound landed, two configurations went from about 0.5x DuckDB to **1.3x**. It
read exactly like a regression. It was the harness: eight threads compared against thirty-two, the
difference called an engine — the same fault as the 38,429-file glob, three items earlier the same
day. The harness now sets `SET threads TO n` on DuckDB and `target_partitions` on DataFusion, and
`THREADS=n` moves all three together.

That is the fourth time today that the thing being measured was the harness. The pattern is worth
naming: **every one of them was invisible to the parity guard**, because in every case the engines
agreed on the answer and disagreed about what question they had been asked.

### The gate, restated

| leg | result |
|---|---|
| latency | **0.38-0.87x DuckDB** across 14 configurations, parity verified on all |
| memory | **210 MB** at 989,690 groups, gate 256, at the default 8-thread budget |

macOS measures 240-246 MB where Linux measures 210, because its allocator returns less. Both pass;
the difference is worth knowing rather than averaging away.

Also worth recording, since the fixture is finally realistic: DataFusion at an equal thread budget is
**3.6x DuckDB at ten thousand segments** — the many-small-files layout a nest actually produces — and
*faster* than DuckDB at a million groups. It is not uniformly the slow one, and saying so is the
difference between an argument and a slogan.

---

## Stage 2.3 — the corpus, and the DuckDB bug it found on its first run — 2026-08-31

`crates/burrmill/tests/slt/` holds hand-computed expectations over tables small enough to check on
paper. They run against Burrmill on every `cargo test`, at three segment layouts, and against DuckDB
through `burrmill-bench slt`. Both green, and mutation-checked: a wrong expected value and a wrong
expected error both fail it.

Expectations are hand-computed rather than recorded from the engine. A corpus whose answers came out
of the thing under test is a regression test — worth having, but it cannot tell you the engine was
ever right.

### Why the standard format, and the vindication

The argument for `sqllogictest` over something bespoke was that DuckDB leaves the graph in Q4, and
every oracle that *is* DuckDB leaves with it, whereas a `.slt` file can be pointed at either engine
and keeps working afterwards. That argument paid on the first run, in a way I did not expect.

**DuckDB silently wraps a `HUGEINT` sum.**

Two rows credit one party with `i128::MAX` and then `1`. The true sum is `MAX + 1`, which no 128-bit
integer holds. DuckDB returns **`i128::MIN`**:

| threads | files | result |
|---:|---:|---|
| 1 | 1, 2, 3 | refuses correctly |
| 2 | 1 | refuses correctly |
| **2, 4** | **2, 3** | **`0xbb = i128::MIN`, wrapped, silently** |

It only goes wrong once the aggregation genuinely runs in parallel, which needs both more than one
thread and more than one file. So the check is in the single-threaded path and missing from the
partial-aggregate combine — and it therefore only misbehaves when the data is big enough to matter.
Measured on libduckdb-sys 1.10501.0, reproducible with `burrmill-bench duckdb-gaps`.

The README hedged that DuckDB "is not watertight everywhere". It now says exactly where, because a
hedge with a reproduction attached is an argument and a hedge without one is a hope. This is the
project's own thesis handed to it by the incumbent: a wrong number that looks exactly like a balance.

The corpus keeps asserting the correct behaviour and carries `skipif duckdb` on that one statement,
with the reason written at the line rather than in a commit message. If `duckdb-gaps` ever prints
"refused" everywhere, the skip comes out.

Filed as 2.3a: reporting it upstream is outward-facing, so it is Chief's call rather than mine.

---

## Stage 2.1 and 2.2 — a generated corpus, and the two things it found — 2026-08-31

Nineteen hand-written refusals and three overflow tests were not a corpus. There are now two oracles,
and they found a semantic bug and a semantic surprise within a few thousand cases.

### Two oracles, because they catch different things

**A non-optimising reference**, in `tests/generated_folds.rs`, running on every `cargo test`: the
same semantics in fifteen lines with a `BTreeMap`, on one thread, straight from the generated rows,
no Parquet and no parallelism. It catches machinery bugs — morsel splitting, the shared aggregate
under locks, the parallel sort — and it survives DuckDB's removal in Q4, which is why it lives in the
library.

It cannot catch a *misconception*. The reference implements this author's reading of what `TRY_CAST`
means, so if that reading is wrong the reference and the engine are confidently wrong together.
Hence **`burrmill-bench gen` against DuckDB**, which is an independent implementation of the standard
and the only thing here that is ground truth about semantics rather than about code.

Three properties per case: the answer matches the reference; the answer does not depend on how rows
were split into segments; the answer does not depend on thread count.

**Mutation-checked, because a generator that goes green on its first run is a clean result nobody
asked for.** Replacing `checked_add` with `wrapping_add` fails it; dropping the last row of every
batch fails it. Both report a reproducible seed.

3,000 DuckDB cases: **2,684 answers agreed exactly, 308 both refused, 8 order-dependent.**

### 2.2 — the boundary is now actually reached

86 of 180 generated cases in the library test reach the refusal path. The benchmark fixture topped
out around 1e20 against an `i128::MAX` of 1.7e38, so no amount of running it could ever have got
there; the boundary was pinned by three hand-written tests and nothing else.

### Finding one: `TRY_CAST` was silently dropping rows

Eight of twenty edge literals differ between `str::parse::<i128>()` and DuckDB's `TRY_CAST`:

| literal | Burrmill | DuckDB |
|---|---|---|
| `" 7"`, `"7 "`, `"\t7"`, `"  -5  "` | NULL | 7, 7, 7, -5 |
| `"1e18"` | NULL | 1000000000000000000 |
| `"7.0"` / `"7.9"` | NULL | 7 / **8** |
| `"1_000"` | NULL | 1000 |

The whitespace group is a plain bug and is fixed: `" 7"` is seven, and Burrmill was skipping the row
and returning a short balance. It is exactly the class that survives a benchmark, because on real
nest data — `uint256` as plain digits — the two are identical.

The rest are not bugs but choices, and DuckDB rounding `"7.9"` to **8** is not one to adopt casually
into an engine whose first claim is exactness. Left open as 2.1a, with `burrmill-bench cast` printing
the whole table so it is visible rather than buried. The generator deliberately does not draw those
four literals: a corpus that fails for a reason nobody has chosen yet is just noise.

### Finding two: refusal is order-dependent, in both engines

The corpus turned up DuckDB refusing where Burrmill answered, and Burrmill refusing where DuckDB
answered. Neither returns a wrong number. They disagree about whether the query is *answerable*.

Checked addition is order-dependent. A party whose values are `i128::MAX, +1, -1` sums to exactly
`i128::MAX` and fits — but any order that meets the `+1` first overflows on the way to an answer it
could have represented.

So **"exact integer arithmetic, refuses on overflow" is a little more eager than it reads**: it
refuses when an intermediate partial sum leaves the range, not when the answer does. The guarantee
that matters is intact — no wrapped number, ever, in either engine — but the README now says this
plainly rather than letting the claim imply more than it delivers.

Fixing it means accumulating wider than `i128`, which costs 16 bytes per group against a memory gate
already being missed at 32 threads. That is a trade, not a patch, so it is 2.1b and
`tests/generated_folds.rs` pins today's behaviour with a test that says in as many words that a
deliberate fix should invert it rather than delete it.

---

## Roadmap 1.4 — the seal-layout canary, and the empty answer it found — 2026-08-31

Burrmill has no path dependency on nuthatch by design, which is what makes the pure-Rust dependency
claim true and lets the two evolve separately. The cost is that a layout change cannot break the
build; it breaks the *reading*, at runtime, quietly. `tests/seal_layout.rs` writes the contract down
as assertions instead of as a paragraph, and `BURRMILL_NEST=<dir>` checks it against a real nest.

### It found a real bug before it found anything about layout

Pointing the harness at a table name that does not exist: **DuckDB refused with "No files found that
match the pattern". Burrmill planned `files=0 morsels=0` and would have returned an empty answer.**

That is the failure this project exists to refuse, in its purest form. "No rows" and "no such table"
are different answers, and the first is a wrong answer that looks entirely plausible — the balances
came back empty, so presumably nobody staked anything. A nest keeps all 34 of its tables' segments in
one directory, so a mistyped name is always exactly one prefix away.

`open_nest_table` now returns `BurrmillError::NoSegments` and names the prefixes that *are* present,
because the mistake is nearly always a near-miss and a list of neighbours turns a puzzled afternoon
into a glance.

### The canary's own grammar was wrong, and that is the point of having one

The contract asserted that a table is `<contract>__<event>`. The first run against a real nest failed
on `grt_total_supply` — a **call** table, sealed from an `eth_call` rather than from a log, carrying
`calldata` / `result` / `reverted` / `content_address` and no event name at all. The grammar was this
crate's assumption, not nuthatch's.

Corrected to what is true: `<table>-<hex content hash>.parquet`, where the table part may or may not
be event-shaped. The event/call mix is now *reported* rather than asserted, because a nest growing a
third shape is news and not necessarily a fault.

Filed as 1.4a: **the fold has only ever run on event tables.** The signed fold has no meaning on a
call table, but the catalog will open one quite happily, and the admitted subset should probably have
an opinion about that.

### The hazard that is not hypothetical

A real nest carries both `staking__stake_delegated` and `staking__stake_delegated_withdrawn`. Bare
`starts_with` would fold the second into the first and report delegations larger than they are — a
wrong balance that reads as perfectly reasonable. The trailing separator is what prevents it, which
makes the separator load-bearing and puts it in a test rather than in a comment.

Verified the canary actually fires: renaming a real segment's separator from `-` to `_` fails it with
"the layout Burrmill assumes has changed and every table would now resolve to zero segments". A guard
nobody has watched refuse is not a guard.

Against the real nest: 38,428 segments, 34 tables, all of them open.

---

## Roadmap 1.1a and 1.1b — making the harness able to catch what it missed — 2026-08-31

Three measurement defects turned up in one day and the parity guard saw none of them, because in
every case both engines agreed on the answer. These two items are the guards for that class, and both
are demonstrated rather than asserted.

### 1.1a — the fixture is now as wide as a real event

The fixture had four columns; a real nuthatch event has twelve, two of them 66-character hex hashes.
The fold reads three, so on a four-column table projection pushdown is worth nothing — which is why
the operator shipped for an entire slice with **no projection at all** and not one measurement
noticed.

Reproduced by deliberately disabling the projection again:

| fixture | projection on | projection off | penalty |
|---|---:|---:|---:|
| narrow, 512 groups | 14 ms | 15 ms | 1.07x |
| **nest-shaped**, 512 groups | 14 ms | 35 ms | **2.5x** |
| narrow, 200k groups | 35 ms | 35 ms | 1.00x |
| **nest-shaped**, 200k groups | 34 ms | 55 ms | **1.6x** |

On the old fixture the defect is unmeasurable. The general rule, worth stating because it is not only
about columns: **a fixture may be unrealistic only in the ways the experiment is about.** Segment
sizes were nest-shaped from day one and the schema was not, and the schema is where the bug lived.

**It also changed the competitive picture, in Burrmill's favour and honestly.** On the wide table
DataFusion goes from roughly 0.8-0.9x DuckDB to **2.0-3.9x**, because a wide table punishes an engine
that decodes columns nobody asked for. Burrmill's ratios improved for the mirror-image reason, to
0.17-0.67. Every published sweep number is restated on the new fixture, and the old ones should not
be compared against: they were measured on a table no nest ever writes.

Widening the schema cost no memory at all, which is the projection working: three columns are decoded
whatever the file holds.

### 1.1b — a ratio dominated by fixed cost is not printed as a number

The nest harness now halves its input and checks whether each engine's time follows, estimating fixed
cost by linear extrapolation from the two points. Crude, and quite enough to tell 5% fixed from 90%.
If more than half of either engine's time is independent of how much data it read, **no ratio is
printed**: the field reads `UNSAFE_fixed_duck=66pct_burr=17pct` and a warning goes to stderr.

On `escrow__deposit` — the table whose 0.11 was this morning's most flattering published figure —
`glob_ratio` is now withheld at 66% fixed, while the like-for-like `ratio=0.48` passes at 8% and 17%.
It refuses exactly the number that was wrong and passes the ones that are not.

Making the wrong figure *unavailable* rather than discouraged is the point. A number that is merely
discouraged is a number somebody pastes into a README.

**Its own first version was wrong in the same way it exists to prevent**: it measured the
explicit-list path and then gated the glob-path ratio, which is a different quantity. Caught the first
time it ran, which is at least the intended failure mode.

### Also fixed: the gate script was inflating its own headline

`run-gate.sh` measured operator RSS with `REPEATS=5`. Allocator retention accumulates across folds
inside one process, so it reported 608 MB where a single fold reports 339. The script now uses
`REPEATS=1` and sweeps thread count, since parallelism is the load-bearing variable the gate does not
name.

### Where the gate stands on the realistic fixture

1M groups, 32-core box, true peak, single fold:

| threads | peak RSS | verdict |
|---:|---:|---|
| 1 | 147 MB | pass |
| 4 | 180 MB | pass |
| 8 | 204 MB | pass |
| 16 | 251-259 MB | **straddles the 256 MB line** |
| 32 | 339 MB | fail |

Sixteen threads gives 251 on one run and 259 on another. That is not a pass and not a comfortable
fail, and declaring either would be picking the run that suits. It is the sharpest argument yet for
1.2c: **a budget with no stated parallelism cannot be evaluated at all.**

---

## Roadmap 1.2 — one aggregate for the query, not one per worker — 2026-08-31

**The shape change is done and holds. The gate is met up to eight threads and missed above sixteen.**
Peak RSS at a million groups fell 2.2x to 3.4x depending on core count, and latency improved at every
core count rather than being traded away.

### What changed

Each worker used to build an aggregation spanning the whole key space, and twelve of them were merged
at the end. A million distinct parties therefore meant roughly twelve million live entries to produce
a 57 MB answer — memory growing with core count, which is exactly the DataFusion behaviour RFC-0044
§4.1 criticises by name (#6937).

Now there is one `SharedAgg`: sixty-four radix partitions, each behind its own lock, written to by
every worker. A key exists in exactly one table however many threads touched it. **`agg_bytes` is 99
MB at one thread and 99 MB at thirty-two**, which is the claim stated as a measurement rather than as
an intention.

The lock is paid for by batching. A `Scatter` buffers rows per partition and drains 4,096 at a time.
The size matters in both directions and the two regimes are far apart: a synthetic segment is 20,000
rows, a real nest segment averages **116**, so flushing per batch would have meant sixty-four lock
acquisitions per hundred-odd rows. The constant was swept rather than guessed; 16,384 was the first
guess and was worse on both memory and latency.

**The merge phase is gone entirely.** There is nothing left to merge; the workers were writing into
the answer as they went.

### Peak RSS and latency, 1M groups, like for like

Same machine (32-core Debian), same fixture, same true high-water mark, single run. The `before`
column is commit `eec0699` built and measured on that same box, because a before-and-after across two
machines and two different definitions of "RSS" is not a comparison.

| threads | before | after | before ms | after ms |
|---:|---:|---:|---:|---:|
| 1 | 538 MB | **146 MB** | 870 | **575** |
| 4 | 557 MB | **186 MB** | 286 | **221** |
| 8 | 584 MB | **210 MB** | 192 | **185** |
| 16 | 608 MB | **259 MB** | 183 | **162** |
| 32 | 769 MB | **349 MB** | 194 | **167** |

Against the 256 MB gate: **pass at 1, 4 and 8 threads; miss by 1% at 16; miss by 36% at 32.**

Two things follow. The first is that the old code was 538 MB *on a single thread*, so thread
duplication was never the whole story — the answer's own representation was most of it. The second is
that **the RFC's gate does not say at what parallelism**, and parallelism is now the load-bearing
variable: the aggregate is flat at 99 MB and everything else costs about 6 MB per thread. A budget
that a build passes on a laptop and fails on a build server is not a budget yet.

### Ratios, unchanged or better

Parity verified on all fourteen sweep configurations. Burrmill ratios 0.16-0.67 against DuckDB, all
comfortably inside the ≤1.0 gate, and better than before at every point except one.

The exception: 512 groups over 100 segments went 0.76 to 0.82. Sixty-four partition tables are pure
overhead for an aggregate that fits in L2 as one, and a shared aggregate cannot use the old promotion
threshold because the partitions are what make it shared. Recorded as a real if small trade rather
than rounded away.

### Three further defects found on the way

1. **The answer allocated a `Box<str>` per group.** The exact per-key malloc this module's header says
   it removed from the table, reintroduced at the output and costing more there. The answer now
   carries one contiguous arena plus a 32-byte index row, and hash tables are freed per partition as
   their rows are built.

   This took three attempts and the middle one was wrong in an instructive way. Keeping the
   sixty-four partition arenas in place and giving each row a partition index avoids a copy, and
   measured **79 ms of output phase against 26 ms** — so the second attempt concatenated them into
   one arena, which sorted fast and allocated a second 42 MB copy of every key to do it. The third
   attempt keeps the arenas where they are and hoists their base pointers into a flat `&[&[u8]]`
   before sorting. That was the entire cause of the 79 ms: indexing a `Vec<Vec<u8>>` inside the
   comparator is two bounds-checked indirections, and sixty-four pointers in a flat slice stay in L1.
   The copy was never necessary; it was paying 42 MB to work around a nested index.

2. **The RSS gate harness was measuring itself.** `fold_only` went through `oracles::burrmill`, which
   collects the answer into `Vec<(String, i128)>` for the parity comparison — a million fresh
   `String`s inside the number that was supposed to be the operator's, worth about 80 MB.

3. **`rss_mb()` reported two different quantities depending on the machine.** `ps -o rss=` on macOS is
   the process's RSS *right now*; `VmHWM` on Linux is a true high-water mark. Peak in a fold happens
   while the aggregate and the answer are both live, and by the time the harness asked, that moment
   had passed — so the development laptop systematically reported the friendlier number. Both now go
   through `getrusage`, and the macOS figure agrees with `/usr/bin/time -l` to the megabyte.

   That is three measurement defects in one day, all the same shape: the harness flattering the thing
   it exists to check. The parity guard cannot see any of them, because in every case both engines
   agreed on the answer.

4. **`max_bytes` was a field in `Limits` that nothing read.** Now enforced against the answer's own
   bytes, which the arena representation makes cheap to compute.

### Where the remaining memory is, and why the obvious fixes are the wrong ones

Three candidates were on the table for closing the gap. Two were ruled out by measurement rather than
by argument, which is the only reason the third is worth doing.

**Not streaming the output.** A globally sorted answer needs every row live whatever you do with it,
so streaming cannot reduce the peak of a `query()` that returns one. It is a serving-path change
(stage 5), not a memory one.

**Not bounding concurrent decode**, which was the first guess and looked obviously right. With a
trivial aggregate the same 2M-row scan costs 24 MB on one thread and 64 MB on thirty-two — **1.3 MB
per thread**. At 989,690 groups the per-thread term is 5.6 MB. The scaling is therefore not the
Parquet decoder at all; it follows the aggregate's size, which means concurrent hash-table growth and
the memory freed behind it.

**Most of the remaining overage is free memory that has not been returned.** The fold frees 57 MB of
hash tables in one go when it turns the aggregate into rows, and glibc holds a free that size rather
than handing it back. Forcing prompt return quantifies it:

| threads | default | `MALLOC_TRIM_THRESHOLD_=131072` | retained |
|---:|---:|---:|---:|
| 8 | 208 MB | 172 MB | 36 MB |
| 16 | 260 MB | 213 MB | 47 MB |
| 32 | 338 MB | 271 MB | 67 MB |

It is still RSS and it still counts. The point of measuring it is that the overage is mostly *not*
live data, so "allocate less" is not the fix — the tables have to exist and then stop existing. A
serving binary removes it with one environment variable. The library does not call `malloc_trim`
itself: reaching into the host's allocator is not a library's business, and it does not exist on
macOS.

Two things also worth knowing about the measurement. `REPEATS` must be 1, because retention
accumulates across folds inside one process and turns 349 MB into 507. And **the gate does not name a
thread count**, which is now the load-bearing omission: the same binary passes at eight threads and
fails at thirty-two. Filed as 1.2c, an RFC amendment rather than a patch.

**Slice 1 remains unpassed.**

---

## Roadmap 1.1 — the real-nest comparison was measuring the wrong thing — 2026-08-31

**The published real-nest ratios were wrong and are restated.** They are also, after a defect the
correction exposed, still under the gate — but by a different margin and for a different reason, and
the two facts have to be reported in that order.

### What the flat ~620 ms was

DuckDB's time on the real nest did not move with rows. Fitting the four recorded points against
segment count alone gives 553 ms fixed plus 0.076 ms per segment, with residuals of 0.2, 0.4, -9.0
and +8.7 ms — a 1.2% fit against file count while row counts ranged over fifty-twofold. A query
engine's time does not do that. Something other than the query was being measured.

It was the catalog. A nest keeps every table's segments in **one** directory, and that directory
holds **38,429 files**; the four tables under test are 2.3% to 7.8% of it. DuckDB's
`read_parquet('<dir>/<prefix>-*.parquet')` re-enumerated and re-matched all 38,429 inside every
timed repeat, while Burrmill's `open_nest_table` did its one `read_dir` *outside* the timer. The
harness was charging one engine for catalog construction and handing the other the same work free.

Decomposed, DuckDB's fixed cost is ~88 ms of `glob()` plus ~178 ms of bind, before a row is read.

### The correction, before any fix

Handing DuckDB the identical file list — same bytes, same plan, no pattern to expand — and running
Burrmill unchanged:

| table | published | like-for-like |
|---|---:|---:|
| `escrow__deposit` | 0.11 | **1.00** |
| `escrow__escrow_collected` | 0.29 | **2.98** |
| `staking__stake_delegated_withdrawn` | 0.61 | **2.12** |
| `staking_legacy__stake_delegated` | 0.71 | **2.24** |

Burrmill was 2.1x to 3.0x *slower* than DuckDB on three of four real tables and level on the fourth.
The 0.11 was 869 files' worth of fold against 38,429 files' worth of directory scan.

### The fifth defect found by measurement: no projection pushdown

The gap the correction exposed had a cause. `fold_morsel` built its reader with `with_row_groups`
and no `ProjectionMask`, so the fold decoded **every column in the segment** and used three. The
synthetic fixture is four columns wide, so this cost essentially nothing and was invisible for the
whole of slice 1. A real nest event is twelve to fourteen columns, two of them 64-character hex
hashes.

Adding the mask cut `scan_ms` by 2.7x to 4.4x: 504 ms to 129 ms on `staking_legacy__stake_delegated`,
173 ms to 39 ms on `escrow__escrow_collected`.

### Corrected figures

Apple M5 Pro, 18 threads, median of 5, parity verified first and separately between DuckDB's glob
and its own explicit file list. `list_ratio` is the like-for-like number the gate applies to.

| table | segments | rows | groups | list_ratio | cold_ratio | published (void) |
|---|---:|---:|---:|---:|---:|---:|
| `escrow__deposit` | 869 | 6,712 | 104 | **0.48** | 0.29 | 0.11 |
| `escrow__escrow_collected` | 905 | 67,004 | 69 | **0.78** | 0.32 | 0.29 |
| `staking__stake_delegated_withdrawn` | 2,875 | 102,766 | 96,353 | **0.76** | 0.46 | 0.61 |
| `staking_legacy__stake_delegated` | 2,985 | 346,288 | 309,664 | **0.77** | 0.49 | 0.71 |

`cold_ratio` is both engines building their own catalog, which is the honest reading of a
cold process; Burrmill wins it because a parallel `read_dir` plus parallel footer parse beats a glob
plus a serial bind. That is a real win and it is a *catalog* win, not a fold win, and it should never
again be quoted as though it were the latter.

The synthetic sweep is unaffected — it uses a purpose-built fixture directory and never touched this
path — and re-running it after the projection change shows no regression: 0.48 at a million groups,
0.82 at 200k, 0.54 at 1000 segments, against 0.46, 0.79 and 0.63 published. `BREAK_PARITY=1` still
refuses with no RESULT line.

### What this does not change

**Peak RSS still fails.** 982 MB on `staking_legacy__stake_delegated` against a 256 MB gate.
Projection pushdown does not touch it, because the memory is in twelve thread-local tables each
spanning the whole key space, not in the decoded columns. Item 1.2 remains the blocker and slice 1
remains unpassed.

### Owed, added

- **The harness gave one engine a catalog and charged the other for it, for a whole slice, and the
  tell was sitting in the results file** — a constant where a slope belonged. Worth a gate that
  refuses a comparison whose timings do not vary with input size, since the parity guard cannot see
  this class of error at all.
- **Projection pushdown existed nowhere and no test would have caught it.** The fixture cannot: it is
  four columns wide by construction. A fixture with a realistic schema width is owed.

---

## Slice 1 — owned operator, in-graph — 2026-08-31

**Gate: PASS on latency, FAIL on peak RSS.** The thesis reproduces; the memory budget does not.

### Latency, against DuckDB (>1.0 means slower)

Segment sweep, 2,000,000 rows, 512 parties, 5 repeats, median, both orderings agree to two decimals:

| segments | DuckDB | DataFusion 55 | Burrmill | ratio |
|---:|---:|---:|---:|---:|
| 1 | 30 ms | 24 ms | 17 ms | **0.57** |
| 100 | 17 ms | 29 ms | 13 ms | **0.76** |
| 1 000 | 71 ms | 49 ms | 45 ms | **0.63** |
| 10 000 | 678 ms | 626 ms | 434 ms | **0.64** |

High-cardinality gate, 100 segments, 2,000,000 rows:

| parties | DuckDB | DataFusion 55 | Burrmill | ratio |
|---:|---:|---:|---:|---:|
| 512 | 17 ms | 29 ms | 13 ms | **0.76** |
| 9 896 | 20 ms | 32 ms | 19–20 ms | **0.95–1.00** |
| 197 936 | 66 ms | 60 ms | 52 ms | **0.79** |
| 989 690 | 162 ms | 121 ms | 74 ms | **0.46** |

Real sealed segments, `graph-allocations-nest`, read-only, nothing written or copied:

| table | segments | rows | parties | DuckDB | Burrmill | ratio |
|---|---:|---:|---:|---:|---:|---:|
| `escrow__deposit` | 869 | 6 712 | 104 | 619 ms | 67 ms | **0.11** |
| `escrow__escrow_collected` | 905 | 67 004 | 69 | 622 ms | 182 ms | **0.29** |
| `staking__stake_delegated_withdrawn` | 2 875 | 102 766 | 96 353 | 762 ms | 467 ms | **0.61** |
| `staking_legacy__stake_delegated` | 2 985 | 346 288 | 309 664 | 788 ms | 559 ms | **0.71** |

Every row above had parity verified before any timing was printed, and `BREAK_PARITY=1` was run to
watch the guard refuse rather than assume it would.

Two caveats on the real-nest numbers, because they flatter. The tables with few rows and many
segments are metadata-bound rather than compute-bound - `escrow__deposit` is six thousand rows spread
over eight hundred and sixty-nine files - so 0.11x is a statement about opening files, not about
folding. And DuckDB's `read_parquet` glob appears to pay a fixed cost around 620 ms on this
directory regardless of table size, which is suspiciously flat and worth understanding before the
number is quoted anywhere.

### Peak RSS: FAIL

Operator only, no DuckDB linked and no DataFusion session, against the 256 MB gate:

| parties | peak RSS | verdict |
|---:|---:|---|
| 509 | 48 MB | pass |
| 197 936 | 682 MB | **fail, 2.7x over** |
| 989 690 | 1 008 MB | **fail, 3.9x over** |

The cause is understood and is not mysterious: twelve workers each build a table spanning the whole
key space, so a million distinct parties means roughly twelve million live entries to produce
989,690 rows. The answer itself is about 57 MB. Everything above that is thread-local duplication -
which is, precisely, the DataFusion behaviour this RFC criticises in §4.1 (#6937, memory growing
with core count). Radix partitioning fixed the *time*; it did not fix this, because partitioning a
worker's table does not stop that worker from seeing every key.

The fix is input repartitioning - workers exchanging morsels so each owns a key range rather than a
row range - and it is a real piece of work, not a tuning knob. Recorded as owed, not as done.

### What was built

- `crates/burrmill` — the library. arrow, parquet, rustc-hash, rayon, sqlparser, hashbrown. No
  DuckDB, no DataFusion, no C++.
- `crates/burrmill-bench` — `publish = false`. Both oracles live here so neither can reach the
  shipped graph.
- The admitted subset and its refusals (19 tests), the owned planner for the signed-fold shape, the
  owned partitioned aggregation, checked i128 arithmetic, canonical ordering, `EXPLAIN`, and a
  read-only real-nest harness.

### Four defects found by measurement, in order

Each of these looked fine and was wrong, and none would have been found by reading the code.

1. **A morsel was "one row group".** `ArrowWriter`'s default row group is 1,048,576 rows, so two
   million rows on a single-segment table was two units of work: the fold ran on two threads against
   DuckDB's twelve. 1.16x. Fixed by making a morsel a bounded row range.
2. **Ten thousand footers parsed serially** before any folding began, a prologue DuckDB does not pay.
3. **A clock read per merged entry** — five million of them at ten thousand morsels, to enforce a
   budget that cannot change that fast.
4. **One hash table per work unit.** Ten thousand tiny segments meant ten thousand fresh maps each
   growing to five hundred entries, so nearly every key allocated its own boxed string rather than
   hitting an existing one: four million allocations for a two-million-row query. The rows had not
   changed, only how they were filed.

Then the aggregation itself, in three measured versions — map-per-unit with a serial merge (3.11x at
200k groups), map-per-thread with a parallel tree merge (1.96x), and arena keys with radix
partitions promoted on demand (0.79x). Promoting unconditionally cost the ten-thousand-group case
1.55x against 0.90x, which is why the table starts as one and splits itself only when it grows.

### Owed

- **Peak RSS at high cardinality** (above). The named blocker.
- The seam, the hot tip, and COR-1 — slice 2, and the highest-risk invariant in the design.
- A test that fails when nuthatch's seal layout changes. Burrmill has no path dependency on nuthatch
  by design, so a layout change will not break the build; it will break the *reading*, silently.
- ~~Differential fuzzing (NoREC/TLP).~~ `burrmill-bench fuzz`, 2026-09-25: differential against
  DuckDB, 8,000 generated cases with no differing answer after nine fixes.
- The DuckDB glob's flat ~620 ms on the real nest directory, understood rather than quoted.
