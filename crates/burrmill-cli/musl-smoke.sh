#!/bin/sh
# The static binary, run: no dynamic section, and a strict, deterministic receipt that verifies.
set -eu
bin=${1:-target/x86_64-unknown-linux-musl/release/burrmill}
data=corpus/receipts/segments
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

if readelf -d "$bin" | grep -q 'NEEDED'; then
    echo "$bin is dynamically linked" >&2
    exit 1
fi
"$bin" q "SELECT who, div(SUM(CAST(amount AS DECIMAL(38,0))), COUNT(*), 4, 'half_even') AS mean FROM transfer GROUP BY who" \
    "transfer=$data" --strict --deterministic --receipt "$out/r.json" > "$out/answer.csv"
"$bin" verify "$out/r.json" "transfer=$data"
for f in json parquet arrow; do
    "$bin" q "SELECT * FROM transfer LIMIT 3" "transfer=$data" --format "$f" > "$out/answer.$f"
done
echo "static, $(wc -c < "$bin") bytes"
