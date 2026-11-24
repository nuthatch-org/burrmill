#!/usr/bin/env bash
# DuckDB's sqllogictest suite against Burrmill (#386). Fetches DuckDB's test/sql at TAG, as data and
# into a temporary directory, runs every .test file through the sqllogictest example, and holds the
# per-directory counts to docs/bench/sqllogictest-baseline.tsv. Nothing of DuckDB's is committed.
#
#   docs/bench/sqllogictest.sh            run, and fail if any directory's wrong answers rose or
#                                         its passes fell (what CI runs)
#   docs/bench/sqllogictest.sh --update   run, and rewrite the baseline and docs/bench/sqllogictest.md
#
# DUCKDB_DIR=<checkout at TAG> reuses a checkout rather than fetching one. SLT_JOBS sets how many
# files run at once (the machine's CPUs unless set). Every record is in target/sqllogictest/records.tsv
# afterwards: file, line, class, detail.
set -euo pipefail

TAG=v2.0.1

root=$(CDPATH='' cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

mode=--check
case "${1:-}" in
  "") ;;
  --update) mode=--update ;;
  *) echo "usage: $0 [--update]" >&2; exit 2 ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/burrmill-slt.XXXXXX")
trap 'rm -rf "$work"' EXIT

src=${DUCKDB_DIR:-}
if [ -z "$src" ]; then
  src=$work/duckdb
  git clone --quiet --depth 1 --branch "$TAG" --filter=blob:none --sparse \
    https://github.com/duckdb/duckdb "$src" 2>/dev/null
  git -C "$src" sparse-checkout set --no-cone '/test/sql/' '/LICENSE'
fi
at=$(git -C "$src" describe --tags --exact-match 2>/dev/null || true)
if [ "$at" != "$TAG" ]; then
  echo "$src is at ${at:-an untagged commit}, not $TAG" >&2
  exit 2
fi

cargo build --locked --release -p burrmill --features datafusion --example sqllogictest
bin=target/release/examples/sqllogictest
out=target/sqllogictest
rm -rf "$out"

# Each file runs in a process of its own; one that allocates without bound fails there, as an error
# for the record it was on, and not the machine.
ulimit -v 4000000
"$bin" run "$src" "$out" --jobs "${SLT_JOBS:-$(nproc)}" --timeout 30

args=(report "$out" docs/bench/sqllogictest-baseline.tsv "$mode")
if [ "$mode" = --update ]; then
  args+=(--doc docs/bench/sqllogictest.md --tag "$TAG" --command "docs/bench/sqllogictest.sh --update")
fi
SLT_DATE=$(date -u +%Y-%m-%d) "$bin" "${args[@]}"
