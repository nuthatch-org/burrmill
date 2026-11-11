# Changelog

Nuthatch pins a git revision of burrmill. Each revision it adopts is tagged `vX.Y.Z` and gets an
entry here, newest first, naming what changed since the last tag. Nothing is published to
crates.io.

## 0.0.1 - 2026-11-11

The first tag, on main as it stands. What the pull requests since #95 changed, #277 the last:

Engine (`burrmill`, feature `datafusion`)

- A batch over 4 MiB is handed to the caller alone, so nothing is built behind it (#273).
- A spill past its disk cap is `LimitExceeded` (#272).
- A statement reads at most 128 relations (#271).
- Rules over shared definitions move the plans they walk (#274).
- Local segments' footers are cached once for the process (#267), a table bound again to the same
  files reuses its file list (#268), and a scan holds pool memory for the segments it has
  opened (#269). RFC-0047 says what a process keeps (#270).
- A statement still planning stops when cancelled; SQL is capped at 1 MiB and error text at
  4 KiB (#252).
- Cancelling one statement stops that statement, and a statement can have a deadline (#239).
- An overflow or a budget refusal is its own error, `Overflow` or `LimitExceeded`, and
  `BurrmillError` is non-exhaustive (#230).
- `Engine::builder()` opens an engine from any source, budget and shared pool (#222).
- A statement can take `$n` placeholders (#233).
- A host can refuse EXPLAIN, which names segment paths (#246).
- A cast to TIMESTAMP is microseconds (#250).
- `list_sum`, `list_min` and `list_max` answer as DuckDB's (#248).
- A refused DuckDB form names what to write instead, and `docs/sql.md` lists what is
  admitted (#232).
- An address decodes only from lower-case hex (#219), and `register_facts` says what declaring
  one promises (#259).
- A hot tip folds only the credit and debit shape, and a cold row without a block is
  refused (#216).
- A shared subquery's readers hold a bounded number of its rows (#228).
- DataFusion 55.2 (#249); hashbrown 0.17 (#231).
- A statement always runs on its own thread, and one its caller stops still reports what it read (#207).
- Only a decode the rewrite placed can turn addresses off (#205).
- A hidden registration name is refused whatever comes before it (#204).
- `levenshtein` refuses past a bounded amount of work, and a cancel frees the gate (#198).
- List comprehensions are found in one pass (#197); a multi-byte character after a bracket no
  longer panics that rewrite (#119).
- Wide nests read footer schemas once per process and canonicalise directories once (#127).
- A union's top-k sorts by its merge's whole order (#126).
- `Engine::result_schema` is public (#124).
- Statement and scan-planning threads get the planner's 8 MiB stack (#123).
- The memory a statement's computed values hold is bounded and charged to the pool (#122, #125).
- A metered statement reports what it selected, read and held (#120), and times its segment scans
  on the thread CPU clock (#121).
- Addresses are fixed binary between the scan and the answer (#116), through recursive queries,
  correlated subqueries and pushed filters (#118); a column holding something else turns that
  off (#117), and `lower()` on one reaches the scan as the column itself (#108).
- A shared subquery too big to hold runs a fresh plan for each reader (#113); a shared definition
  is cut to what its readers take (#102); a repeated window is computed once (#103).
- A budgeted session reads 2,048 rows at a time (#112).
- Running max and min over a growing frame are one pass per partition (#107), and plan under one
  target partition (#111).
- RFC-0046: what sealed history has already answered is kept (#104).
- A symlinked segment is sized by its target (#105); `SegmentTable` reads the session's footer
  cache (#101).
- A view's or derived table's ORDER BY orders the answer when nothing above reorders (#95).
- Runs in a browser tab: an async query path with no tokio spawn on wasm32 (#106, #109).

Ingest and the tab (`burrmill-ingest`, `burrmill-wasm`, `demo/`)

- Ingest opens Parquet (#257) and JSON (#262) under the same budgets as CSV. A Parquet page
  claiming more than the budget (#261) and a list or map column (#266) are refused first.
- A first row is taken as names only when the rows under it are not all text (#238).
- A decimal with a negative scale prints with its zeros appended (#234).
- Aggregates mode returns the statement's own groups and still counts every small one (#221).
- Pseudonymised mode finds a tokenised value by rolling hash (#218) and deals token numbers at
  random (#256).
- In schema mode only a statement that does not plan goes back to the model (#240).
- The demo page passes an axe run (#241), fits a 360 px screen, exports CSV and runs edited
  SQL (#255), and requires Trusted Types, caches only its shell and budgets aggregate
  asks (#260).
- The tab is described in `docs/browser.md` (#235).
- Untrusted CSV becomes Arrow in a browser tab, and the demo page asks a model about it in four
  privacy modes (#128).
- Values are no longer changed silently: decimal marks per column, real dates, offsets, leading
  zeros (#195).
- The memory budget counts empty fields, rows and the built columns (#196).
- A sniff window that splits a character still reads as UTF-8, and delimiter sniffing is
  linear (#208).

Build and CI

- A nightly job fuzzes each target for ten minutes, SQL among them (#251, #258, #263, #264).
- CI runs every demo browser test against the built engine (#242, #277) and keeps the wasm32
  build free of warnings (#265).
- Every crate sets `publish = false` (#229). Dependabot leaves what moves with DataFusion, and
  the DuckDB oracle, alone (#217, #237, #247).
- The README's QoS and allocations figures are re-measured (#276), and the docs describe the
  tree as it stands (#220).
- Arrow 59.3.2; zstd and tokio move past their advisories (#202).
- CI checks dependencies with cargo deny, runs read-only, and pins the toolchain (#206).
- CI tests `burrmill-wasm` and gates the tab's engine at 8 MiB gzipped (#192).
- The bench allocates as nuthatch does (#114); where the time goes is measured (#115).
- The organisation is `nuthatch-org` (#96).
