#!/bin/sh
# RFC-0047. Gzip of ingest plus the operator, of the engine, and of both.
# The 8 MB figure is the gate for whichever of these the demo actually downloads.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

# Printed after the sizes. sqlparser's default `recursive-protection` links psm,
# whose build script compiles an assembly stub with cc. That is a purity failure
# the size still has to be measured past: the 8 MB gate is a number, not a guess.
pure=0
check_tree() {
    label=$1
    shift
    tree=$(cargo tree -p burrmill-size --locked --features link-wasm \
        --target wasm32-unknown-unknown --edges normal,build --prefix none "$@")
    bad=$(printf '%s\n' "$tree" | awk '{print $1}' | sort -u | awk '
        $0 == "js-sys" { next }
        $0 ~ /-sys$/ || $0 == "cc" || $0 == "cmake" { print }
    ')
    if [ -n "$bad" ]; then
        pure=1
        printf '%s wasm graph is not pure Rust:\n%s\n' "$label" "$bad" >&2
    fi
}

if [ "${1:-build}" = "tree" ]; then
    check_tree operator
    check_tree engine --features engine
    exit "$pure"
fi

measure() {
    wasm=$1
    raw=$(wc -c < "$wasm" | tr -d ' ')
    gz=$(gzip -9 -c "$wasm" | wc -c | tr -d ' ')
    printf '%s\nuncompressed %s\ngzip-9 %s\n' "$wasm" "$raw" "$gz"
}

cargo build -p burrmill-size --locked --target wasm32-unknown-unknown \
    --features link-wasm --bin ingest-operator --profile wasm-size
measure target/wasm32-unknown-unknown/wasm-size/ingest-operator.wasm

# Apple clang has no wasm32 target. zstd-sys needs one; psm already linked without it.
if ! printf 'int x;\n' | clang --target=wasm32-unknown-unknown -c -x c - -o /dev/null 2>/dev/null; then
    llvm=/opt/homebrew/opt/llvm/bin/clang
    if [ ! -x "$llvm" ]; then
        printf 'no clang that accepts wasm32-unknown-unknown\n' >&2
        exit 1
    fi
    export CC_wasm32_unknown_unknown="$llvm"
fi

cargo build -p burrmill-size --locked --target wasm32-unknown-unknown \
    --features engine --bins --profile wasm-size
measure target/wasm32-unknown-unknown/wasm-size/engine-wasm.wasm
measure target/wasm32-unknown-unknown/wasm-size/ingest-engine.wasm

check_tree operator
check_tree engine --features engine
exit "$pure"
