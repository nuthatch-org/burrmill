#!/bin/sh
# demo/pkg: burrmill-wasm for wasm32 and its JS glue. PROFILE=wasm-size for the shipped size.
# The browser tests then run against a static server:
#   (cd demo && npm ci && python3 -m http.server 18747 --bind 127.0.0.1) &
#   for t in demo/*.mjs; do node "$t" 18747; done
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"
profile=${PROFILE:-release}

want=$(sed -n 's/^wasm-bindgen = "=\(.*\)"$/\1/p' crates/burrmill-wasm/Cargo.toml)
have=$(wasm-bindgen --version | awk '{print $2}')
if [ "$have" != "$want" ]; then
    printf 'wasm-bindgen %s is on PATH; the crate pins %s:\n  cargo install wasm-bindgen-cli --version %s --locked\n' "$have" "$want" "$want" >&2
    exit 1
fi

# Apple clang has no wasm32 target, and zstd-sys compiles C.
if ! printf 'int x;\n' | clang --target=wasm32-unknown-unknown -c -x c - -o /dev/null 2>/dev/null; then
    llvm=/opt/homebrew/opt/llvm/bin/clang
    if [ ! -x "$llvm" ]; then
        printf 'no clang that accepts wasm32-unknown-unknown\n' >&2
        exit 1
    fi
    export CC_wasm32_unknown_unknown="$llvm"
fi

cargo build --locked -p burrmill-wasm --target wasm32-unknown-unknown --profile "$profile"
dir=$profile
[ "$profile" = dev ] && dir=debug
wasm-bindgen --target web --out-dir demo/pkg "target/wasm32-unknown-unknown/$dir/burrmill_wasm.wasm"
