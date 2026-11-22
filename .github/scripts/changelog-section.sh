#!/bin/sh
# Print one version's section of CHANGELOG.md, without its heading: from the line after
# "## X.Y.Z - date" up to the next "## " heading, with blank lines trimmed at both ends.
# Exits 1 if the version has no entry. Usage: changelog-section.sh X.Y.Z [CHANGELOG.md]
set -eu
version=${1:?usage: changelog-section.sh X.Y.Z [CHANGELOG.md]}
file=${2:-CHANGELOG.md}
out=$(awk -v v="$version" '
    /^## / { if (on) exit; split($0, h, " "); on = (h[2] == v); found = found || on; next }
    on { print }
    END { exit found ? 0 : 1 }
' "$file") || { echo "no CHANGELOG entry for $version in $file" >&2; exit 1; }
body=$(printf '%s\n' "$out" | sed -e '/./,$!d')
[ -n "$body" ] || { echo "CHANGELOG entry for $version is empty" >&2; exit 1; }
printf '%s\n' "$body"
