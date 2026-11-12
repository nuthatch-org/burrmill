# The browser tab

What the demo page and `burrmill-wasm` do today, read from the code. A CSV, JSON or Parquet file
is read and queried inside the tab; a model is asked for SQL, and only what is listed below goes to
it. The page does the HTTP (`demo/app.js`); the crates do none.

## Pieces

- `crates/burrmill-ingest`: untrusted CSV, Parquet with its `parquet` feature and JSON with `json`,
  to Arrow under a budget. It does not depend on the engine. JSON is an array of objects or objects
  one after another; a key is a column name, so it is part of the schema and leaves with it, and a
  nested value stays in its cell as text. A Parquet file with a list or map column is refused,
  since nothing in the file bounds what one decodes to; a struct of plain fields is read as text.
- `crates/burrmill-wasm`: the JS package. Ingest, the engine (`Engine::open_empty_within`, one table
  `t`), the card, the checks on each mode's feedback and on the model's answer (`src/session.rs`,
  `src/aggregates.rs`, `src/pseudonym.rs`, `src/redact.rs`, `src/model.rs`, `src/chart.rs`).
- `crates/burrmill-size`: links ingest with the operator or the engine so the wasm gzip can be
  measured (`docs/bench/ingest-wasm.sh`, `docs/bench/engine-wasm.sh`). Not a JS API.
- `demo/`: the page (`index.html`, `app.js`), the worker that holds the session (`worker.js`), the
  offline shell (`sw.js`), and the browser tests (`*.mjs`).

## What leaves in every mode

Each request to the model (`promptText` and `requestBody` in `demo/app.js`) carries:

- the card (`Session::card`): table name, row count, CSV dialect (`parquet` or `json` for those
  files), each column's name, inferred type, null count and note, and ingest issues as row number,
  column and reason. No cell value.
  For a column the user ticks as a sign, one word for its sign; for one ticked as a date, its
  minimum and maximum;
- the grammar (`Session::grammar`): the dialect (DuckDB), the admitted statement kinds, the DuckDB spellings the engine rewrites (`Engine::duckdb_spellings`), the few it refuses by name, and the registered function names that answer over exact types;
- the question, and on a retry the model's own previous statement with that attempt's feedback.

Feedback after a query that ran is its shape (`feedback_shape`): row count, column names and types,
and whether it was truncated. A query that failed before reading a row sends its message through
`redact`, which replaces stored cell strings and quoted spans of four characters or more; one that failed while reading rows sends only a
fixed kind (`cast_failed`, `divide_by_zero`, `overflow`, `too_big`, `stopped`, `failed`), from
`Session::failure`. Every statement is first held to table `t` by `inspect::reach_uploads`.

The answer is run, charted and narrated in the tab (`Session::present_bound`); its rows are not sent.
Its statement can be edited and run again with "Run here", which queries the tab and sends nothing;
the model's narration is dropped, since it was written for the other statement. "Download CSV" saves
the rows on the page, with a `'` before any cell that a spreadsheet would read as a formula.

## The four modes

- **Schema** (the default). Only the above, and a second request only after a failure before any
  row was read (`Failure::before_rows`). A statement that finds no rows, or fails while reading
  them, is shown on the page and nothing more is sent, so whether another request arrives depends
  on the schema alone.
- **Aggregates**, k = 10 (`AGGREGATE_K`). The model's statement is rewritten by `measure_sql`: it
  must be one grouped read of `t`; `HAVING`, `LIMIT` and any `group_n` it wrote are removed, and
  `count(*) AS group_n` is added. Each output must be a grouping key, or `count`, `sum` or `avg` of a
  column with arithmetic, `CAST` and `round` over them; `DISTINCT` only in `count`; `FILTER` and
  `CASE` inside an aggregate are refused, as is a CTE named `t`. The feedback (`filter_payload`) is
  the groups with `group_n` of at least k and the number dropped. Anything else sends no group and
  a reason.
- **Pseudonymised**. The user marks identifier columns (`Session::bind`; `Session::proposals` offers
  them). Each distinct marked value is a token such as `CUSTOMER_3`; the map stays in the tab
  (`pseudonym::assign`) and narration restores values locally. `projection_allows` refuses an
  expression over a marked column, a marked column (or a name renamed from one) in `WHERE`,
  `HAVING`, `QUALIFY`, a join condition or a `NATURAL` join, anything but a bare key in `GROUP BY`,
  and `* REPLACE`, `* RENAME` and a star alias. If allowed, the rows go with marked values as tokens;
  a cell that still looks like an identifier, or contains a marked value of eight characters or
  more, sends no rows (`mask`). With
  nothing marked, no rows are sent.
- **Local model**. Requests go to `http://127.0.0.1:<port>/v1/messages` with no key, and carry
  `SELECT * FROM t` as JSON, up to the row cap. The model field holds a name the local server has
  (it starts empty in this mode), and the server must allow the page's origin: for Ollama, set
  `OLLAMA_ORIGINS`. A server that does not shows as a failed fetch.

## Budgets

- Ingest, `Budget::untrusted` (`crates/burrmill-ingest/src/budget.rs`): 200 MiB of input, 5,000,000
  rows, 1,024 columns, 1 MiB per cell, 512 MiB of memory, a 64 KiB sniff.
- Engine: 256 MiB (`MEMORY_BYTES` in `crates/burrmill-wasm/src/session.rs`), one partition.
- Rows handed to the page from one query: 5,000 (`RESULT_ROWS`).
- The page: a file over 200 MB is refused before it is read (`MAX_FILE_BYTES`); a request asks for
  at most 2,048 tokens; four attempts per question; the outbound log keeps eight requests, each
  shown to 64 KB; aggregates mode takes 20 asks per page load (`AGGREGATE_ASKS`), counted on the
  page, which bounds differencing across asks without preventing it.
- A statement running past 60 s (`QUERY_MS`), or one the user stops with "Stop the query", ends the
  worker; the page starts a new one and opens the held file in it again, so the file and its
  renames survive.
- The gzip of ingest plus the engine for wasm32 must be at most 8 MiB (`tab-wasm` in
  `.github/workflows/ci.yml`).

## What the page enforces

From `demo/index.html` and `demo/app.js`:

- A Content Security Policy: `connect-src` is the page's origin, `https://api.anthropic.com` and
  `http://127.0.0.1:*`; scripts from the origin only, with wasm compilation allowed; no objects, base or form targets;
  Trusted Types required for script sinks. The meta tag carries what a meta tag can; `demo/_headers`
  is the same policy as headers, adding `frame-ancestors 'none'` and covering `worker.js`, for a host
  that reads that file (`headers.mjs` serves it). The page should have an origin of its own, since
  OPFS and Cache Storage are shared across one.
- A Trusted Types policy that refuses HTML and script, and script URLs except same-origin `.wasm`,
  `worker.js` and `sw.js`.
- The service worker answers and caches only the files in its `SHELL` list.
- The first local-model ask adds a second policy holding `connect-src` to the origin and
  `http://127.0.0.1:*`, so the document cannot reach `api.anthropic.com` until it is reloaded, and
  an ask in another mode after it is refused.
- Requests with `redirect: 'error'` and `credentials: 'omit'`.
- The key stays in the tab, and in `sessionStorage` only if "Remember the key in this tab" is ticked.
- "Confirm before sending" is on by default; each request is shown in the log and waits for Send or
  Don't send.
- A 4xx other than 429 stops the question with the API's message. A 429, a 529 or a 5xx waits (`retry-after` if sent, else a doubling pause) and sends the request again without telling the model; if the attempts run out the page shows the API's message.
- The local model's port must be 1 to 65535.
- A column name or question matching an identifier label (email, phone, iban, ssn, patient, hiv,
  id) shows a warning. It does not stop a send.
- A file is stored on the machine (OPFS) only if "Keep this file on this machine" is ticked, and
  only once it opens.

## Tests

- Rust, run by `cargo test -p burrmill-wasm` in CI's `tab-wasm` job: what each mode sends is checked
  against canary values in `src/session.rs`, `src/aggregates.rs` and `src/pseudonym.rs` (for example
  `the_card_has_no_cell_and_a_sign_has_no_magnitude`, `a_member_value_does_not_leave_through_a_large_group`,
  `an_identifier_cannot_choose_rows`, `an_error_while_reading_rows_sends_its_kind_not_its_message`).
- The page: `demo/egress.mjs` drives the page in Chromium with Playwright and fails if a canary cell
  appears in any request body or in the log, if the key is in a body, or if any host other than
  localhost and `api.anthropic.com` is contacted. `aggregates.mjs`, `pseudonym.mjs`, `local.mjs`,
  `send.mjs`, `warn.mjs`, `offline.mjs` and `persist.mjs` cover the other modes and controls, and
  `a11y.mjs` fails on any axe violation. CI's `demo` job builds the engine with `demo/build.sh` and
  runs every `demo/*.mjs`.
