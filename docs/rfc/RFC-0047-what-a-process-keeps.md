# RFC-0047: What a process keeps of its sealed segments

- Status: Draft, 2026-11-11. §2 is implemented (burrmill#267, burrmill#268); §4 is not started and
  not committed to. Answers burrmill#161.
- Depends on: burrmill#127 (footer schemas read once per process), which §2 extends; RFC-0045 §3.1
  and §3.3, which §4 refers to.

## §0 What this RFC is for

Burrmill#161 observes that footers, schemas and file lists belonged to one engine and one bind,
although a sealed segment never changes, and asks which parts of a process-wide catalogue to take
on and in what order. Nuthatch opens a new engine at every seal and binds every table again before
each statement, so whatever an engine or a bind keeps to itself is paid for again each time.

This RFC takes on two parts, both landed, and names three as later work.

## §1 What was measured

`burrmill-bench host-scan` drives the path nuthatch drives: an empty budgeted engine, each table
bound with `register_facts`, three engines in one process. Each engine counts every table, which
reads nothing of a segment but its footer. Figures are medians on the ThinkPad on 2026-11-11, main
against main with #267, #268 and #269, on a box under load (§5).

| nest copy | segments | tables | a new engine's first count of every table | a warm count | one rebind of every table |
| --- | ---: | ---: | ---: | ---: | ---: |
| allocations (`1165-corpus`) | 1,924 | 81 | 1,179 to 756 ms | 840 ms | 222 to 137 ms |
| allocations (gate copy) | 6,753 | 83 | 1,935 to 1,174 ms | 1,032 ms | 246 to 189 ms |
| QoS (gate copy) | 14,913 | 6 | 3,996 to 4,924 ms | 3,876 ms | 169 to 13 ms |

## §2 What is taken on

### 2.1 One footer cache for local segments (#267)

Scans of segments on this machine read one `FileMetadataCache` for the process. It is keyed by the
segment's absolute path, and an entry answers only for the size it was read at. Its limit is the
largest eighth that any live engine's budget sets aside for footers, lowered when that engine
closes; when the last engine closes the limit stays, so a host that closes one engine before
opening the next keeps the footers between them.

Segments read through an engine's object store stay in that engine's own cache. A store key means
what that engine's store says, so two engines may hold different files under one key.

### 2.2 File lists reused while a table's file set is unchanged (#268)

`register_facts` keeps the list a scan reads, and the distinct footer schemas of its files, for as
long as some table in the process is bound to it. A later bind finds lists by the first file and
compares the whole list, path and size; an equal one is reused, on the same engine or another. The
process holds only weak references, so a list is freed with the last table bound to it, and a
changed file set builds a new list.

One behaviour changes: a file removed after its list was built refuses the scan that opens it,
where it used to refuse the bind.

## §3 What these two do not do

The QoS row does not move, and its warm count is no faster than its cold one. Its 14,913 footers
fill the cache at the nest's 704 MB budget (an 88 MB limit) and at 2 GB (256 MB) alike, so every
pass over the nest reads every footer again, in one engine or in several. A shared cache changes
nothing while the footers do not fit it.

On the allocations gate copy a rebind still costs 189 ms for 83 tables, where QoS's six cost 13:
what is left goes with the number of tables and not of segments. `register_facts` plans the
table's view at every bind.

## §4 Later work, named and not committed to

- Seal-manifest statistics on each `PartitionedFile`, which RFC-0045 §3.1 names for day pruning.
  §3's QoS figure is the measurement to set beside it.
- A key that survives a rebind, which the view plan cache of RFC-0045 §3.3 needs. §3's 189 ms is
  the measurement to set beside it.
- One scan shared by a statement's repeated scans of one table.

No order is fixed among these and none is scheduled.

## §5 How the numbers were taken

`burrmill-bench host-scan <nest> <queries.tsv>` with `STATEMENT_ROUNDS=0`, five rounds, each binary
twice and the two interleaved under one lock; 2 GB and eight threads for the allocations copies,
704 MB and four threads for QoS. The table gives the first run's medians, and
`docs/bench/host-scan-thinkpad-2026-11-11.txt` has every run. The ThinkPad's load average was 64
to 100 throughout, from work that was not ours to stop, so the pairs are what to read.
