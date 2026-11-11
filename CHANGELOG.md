# Changelog

Nuthatch pins a git revision of burrmill. Each revision it adopts is tagged `vX.Y.Z` and gets an
entry here, newest first, naming what changed since the last tag. Nothing is published to
crates.io.

## 0.0.1 - 2026-11-11

The first tag, on main as it stands. What the pull requests since #95 changed:

Engine (`burrmill`, feature `datafusion`)

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

- Untrusted CSV becomes Arrow in a browser tab, and the demo page asks a model about it in four
  privacy modes (#128).
- Values are no longer changed silently: decimal marks per column, real dates, offsets, leading
  zeros (#195).
- The memory budget counts empty fields, rows and the built columns (#196).
- A sniff window that splits a character still reads as UTF-8, and delimiter sniffing is
  linear (#208).

Build and CI

- Arrow 59.3.2; zstd and tokio move past their advisories (#202).
- CI checks dependencies with cargo deny, runs read-only, and pins the toolchain (#206).
- CI tests `burrmill-wasm` and gates the tab's engine at 8 MiB gzipped (#192).
- The bench allocates as nuthatch does (#114); where the time goes is measured (#115).
- The organisation is `nuthatch-org` (#96).
