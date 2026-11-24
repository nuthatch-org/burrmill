# Reading a 1 GB CSV, against DuckDB's read_csv (#389, #407)

Measured 2026-11-24 on a 32-thread Linux 6.12 machine (i9-13980HX) with 62 GiB, release build. The
CLI table below was measured on 2026-11-23, when the machine was shared and loaded. Each figure is
the best of three warm runs, and every run of the bench is shown. `burrmill-bench csv-ingest`
writes the CSV into a temporary directory (the same bytes on every machine), checks that both
engines answer one statement over every column alike, then times each reading the whole file into
an in-memory table, at `THREADS` and at one thread, and deletes the file:

    cargo build --locked --release -p burrmill-bench
    target/release/burrmill-bench csv-ingest     # BYTES=1000000000 THREADS=8 RUNS=3 BUDGET=4GiB

The file is 1,000,000,010 bytes, 22,863,606 rows of `id,amount,day,who,qty,ok`. Burrmill reads it
through `EngineBuilder::csv`, which is `burrmill-ingest`, the tab's reader, under a budget of 4 GiB
(the CLI's default `--memory`) and the thread count shown. DuckDB is the bench's bundled 1.5.5,
`CREATE TABLE AS SELECT * FROM read_csv(...)`, after `SET threads`.

## Since #407

Three runs of the bench, a minute apart, on a quiet machine (1-minute load under 4 at the start of
each), seconds each (best of three):

| run | burrmill 8 threads | duckdb 8 threads | ratio | burrmill 1 thread | duckdb 1 thread | ratio |
|---|---:|---:|---:|---:|---:|---:|
| 1 (load 3.5) | 1.31 | 0.80 | 1.64 | 7.07 | 5.19 | 1.36 |
| 2 (load 2.9) | 1.32 | 0.79 | 1.68 | 7.11 | 5.17 | 1.37 |
| 3 (load 2.4) | 1.31 | 0.78 | 1.67 | 7.09 | 5.18 | 1.37 |

**Burrmill is within 1.7 times DuckDB at 8 threads and 1.4 times on one thread.** At 8 threads
Burrmill reads at 760 MB/s, against 38.4 MB/s before.

Through the CLI, `burrmill q "SELECT COUNT(*) AS n FROM trades" trades.csv` at the default
`--memory` (4 GiB), timed with `getrusage` around the process, best of three:

| `--threads` | wall | user CPU | system CPU | peak RSS |
|---:|---:|---:|---:|---:|
| 8 | 1.77 s | 9.38 s | 0.73 s | 1.90 GiB |
| 1 | 7.86 s | 7.24 s | 0.61 s | 1.91 GiB |

The peak is the file's bytes (1.0 GB), held while it is read and hashed, and the table's Arrow.
The bench's whole process, DuckDB's tables included, peaked at 3.54 GiB.

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

What is left behind DuckDB: one thread parses at 137 MB/s against DuckDB's 190 MB/s, and at 8
threads one of the eight hashes the file, so seven parse.

## Before #407

The reader was built for a file a stranger drops into a tab, at most 200 MiB. `EngineBuilder::csv`
took 26.06 s for this file on one thread (38.4 MB/s) under an 8 GiB budget, against DuckDB at
0.74 s on 8 threads and 5.16 s on one; it counted more than 4.5 GB for the file, so at the default
4 GiB it was refused (`budget max_memory_bytes exceeded: limit 4294967296, at 4535643211`), and a
file over 1 GiB was refused outright, since a text column was one Arrow array with 32-bit offsets.

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
