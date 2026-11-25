# Reading a 1 GB CSV, against DuckDB's read_csv (#389, #407, #414)

Measured 2026-11-25 on 2d44933, on a 32-thread Linux 6.12 machine (i9-13980HX) with 62 GiB, release
build, against **DuckDB 2.0.1** on CPU, the release CLI (`duckdb --version`:
`v2.0.1 (Cyanoptera) 8d78114e90`). The CLI table below was measured on 2026-11-23 and has no
DuckDB in it. Each figure is the best of three warm runs, and every run of the bench is shown.
`burrmill-bench csv-ingest` writes the CSV into a temporary directory (the same bytes on every
machine), checks that both engines answer one statement over every column alike, then times each
reading the whole file into an in-memory table, at `THREADS` and at one thread, Burrmill first and
DuckDB straight after, and deletes the file:

    cargo build --locked --release -p burrmill-bench
    DUCKDB=/path/to/duckdb target/release/burrmill-bench csv-ingest     # BYTES=1000000000 THREADS=8 RUNS=3 BUDGET=4GiB

The file is 1,000,000,010 bytes, 22,863,606 rows of `id,amount,day,who,qty,ok`. Burrmill reads it
through `EngineBuilder::csv`, which is `burrmill-ingest`, the tab's reader, under a budget of 4 GiB
(the CLI's default `--memory`) and the thread count shown. DuckDB 2.0.1 is the CLI run as a process
(`DUCKDB=`), `CREATE OR REPLACE TABLE t AS SELECT * FROM read_csv(...)` then `DROP TABLE t`, after
`SET threads`, its time what its `.timer` reports for the two. Without `DUCKDB` the bench uses its
bundled DuckDB, which reports v1.5.4.

## Against DuckDB 2.0.1

Three runs of the bench, back to back, on the shared machine, busy with other work: the 1-minute
load at the start and end of each is given. Seconds each, best of three:

| run (load start / end) | burrmill 8 threads | DuckDB 2.0.1 8 threads | ratio | burrmill 1 thread | DuckDB 2.0.1 1 thread | ratio |
|---|---:|---:|---:|---:|---:|---:|
| 1 (6.7 / 8.4) | 1.87 | 0.76 | 2.46 | 7.67 | 4.68 | 1.64 |
| 2 (8.1 / 24.2) | 1.39 | 1.12 | 1.24 | 7.26 | 6.91 | 1.05 |
| 3 (24.2 / 18.8) | 1.60 | 0.80 | 1.99 | 7.51 | 4.70 | 1.60 |

Every run gave the same answer from both: `22863606`, `261372251093421`, `1143191597145.51`,
`2024-01-01`, `w999`, `11420366473`, `7621202`.

**Burrmill takes 2.0 to 2.5 times DuckDB 2.0.1's time at 8 threads and 1.6 times on one thread,**
leaving out run 2, where the load rose to 24 while DuckDB ran. At 8 threads Burrmill read at 534
to 720 MB/s here; on 2026-11-24, on a quiet machine (load under 4), it read at 760 MB/s (1.31 to
1.32 s in three runs, against the bundled DuckDB 1.5.4's 0.78 to 0.80 s), against 38.4 MB/s before
#407.

Through the CLI, `burrmill q "SELECT COUNT(*) AS n FROM trades" trades.csv` at the default
`--memory` (4 GiB), timed with `getrusage` around the process, best of three:

| `--threads` | wall | user CPU | system CPU | peak RSS |
|---:|---:|---:|---:|---:|
| 8 | 1.77 s | 9.38 s | 0.73 s | 1.90 GiB |
| 1 | 7.86 s | 7.24 s | 0.61 s | 1.91 GiB |

The peak is the file's bytes (1.0 GB), held while it is read and hashed, and the table's Arrow.
The bench's whole process, the bundled DuckDB 1.5.4's tables included, peaked at 3.54 GiB on
2026-11-24.

Types: `id` BIGINT, `amount` DECIMAL(7,2), `day` DATE, `who` VARCHAR, `qty` BIGINT, `ok` BOOLEAN.
DuckDB reads `amount` as DOUBLE; the parity statement sums it cast to DECIMAL(18,2), and both
answer `1143191597145.51`.

**How it reads.** The text is cut into chunks of 4 MiB at line ends. A chunk's start is a guess,
the first line end after its offset; the chunk before it, read from a row start it knows, says
where its rows really end, and a chunk whose guess was wrong (a quoted field ran across the offset)
is read again from there, so a quoted line end is never a row end. The first chunk is read alone
and gives a guess at every column's type; the other chunks are then read at once, each counting
what its cells read as and building an Arrow batch under the guess. Each column's type is chosen
from the counts over the whole file, the same rule the tab applies, and where the guess was that
type (as it is for this file: 238 of 239 batches stood) the batches are kept; any other chunk is
built again under the file's types. A cell its column's type does not read, however far into the
file, is reported at its row and column, and the engine refuses the file there. A batch is a chunk's rows in file order, so the table, the
receipt (`csv:<sha256>:<bytes>`) and every refusal are the same at any thread count. The file's
hash is taken on one of the budget's threads beside the parse, not in a second pass after it.
Memory goes through the budget: batches built under the guess stop once they would pass what is
left of it, the counts give an estimate of the whole table that is charged before the rest is
built, and a file whose table would not fit is refused.

What is left behind DuckDB: one thread parses at 130 to 138 MB/s against DuckDB 2.0.1's 213 to
214 MB/s (145 in run 2), and at 8 threads one of the eight hashes the file, so seven parse.

## Before #407

The reader was built for a file a stranger drops into a tab, at most 200 MiB. `EngineBuilder::csv`
took 26.06 s for this file on one thread (38.4 MB/s) under an 8 GiB budget, against the bundled
DuckDB 1.5.4 at 0.74 s on 8 threads and 5.16 s on one; it counted more than 4.5 GB for the file, so
at the default 4 GiB it was refused (`budget max_memory_bytes exceeded: limit 4294967296, at
4535643211`), and a file over 1 GiB was refused outright, since a text column was one Arrow array
with 32-bit offsets.

Where the time went, timed by phase on main (`d765b94`), `burrmill-ingest` alone, one run at load
8 to 12: 35.4 s in all, 32.3 s of user CPU, 6.32 GiB peak RSS.

| phase | seconds |
|---|---:|
| copy the bytes | 0.21 |
| sniff the encoding, decode to a `String` | 0.70 |
| parse every row into its fields | 6.60 |
| classify each column's cells, a column at a time | 14.77 |
| build each column's array | 1.25 |
| the rest of typing: a locale vote over every cell, the header, freeing the rows | 11.9 |

Four fifths of it was typing, over rows already parsed and held: each column walked all 22.9
million rows again, and the locale vote walked them once more. The reader now counts a cell's type
as it parses it, once, and builds the batch in the same pass.
