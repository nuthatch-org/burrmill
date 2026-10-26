#!/bin/sh
# RFC-0047 slice 2. The wasm32 graph of burrmill-ingest has no C, and the
# linked reader has a gzip size. The 8 MB demo gate is a later slice.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

tree=$(cargo tree -p burrmill-ingest --locked --target wasm32-unknown-unknown \
    --edges normal,build --prefix none)
bad=$(printf '%s\n' "$tree" | awk '{print $1}' | sort -u | awk '
    $0 == "js-sys" { next }
    $0 ~ /-sys$/ || $0 == "cc" || $0 == "cmake" { print }
')
if [ -n "$bad" ]; then
    printf 'wasm graph is not pure Rust:\n%s\n' "$bad" >&2
    exit 1
fi

if [ "${1:-build}" = "tree" ]; then
    exit 0
fi

cargo build -p burrmill-ingest --locked --target wasm32-unknown-unknown \
    --features link-wasm --profile wasm-size
wasm=target/wasm32-unknown-unknown/wasm-size/ingest-wasm.wasm
raw=$(wc -c < "$wasm" | tr -d ' ')
gz=$(gzip -9 -c "$wasm" | wc -c | tr -d ' ')
printf 'uncompressed %s\ngzip-9 %s\n' "$raw" "$gz"
