#!/bin/sh
# Install a prebuilt burrmill from GitHub Releases.
#   curl -fsSL https://raw.githubusercontent.com/nuthatch-org/burrmill/main/install.sh | sh
# BURRMILL_VERSION=vX.Y.Z picks a release (default: the latest);
# BURRMILL_INSTALL_DIR picks the directory (default: $HOME/.local/bin).
# The archive's sha256 is checked against the release's SHA256SUMS before anything is installed.
set -eu

repo=nuthatch-org/burrmill
dir=${BURRMILL_INSTALL_DIR:-$HOME/.local/bin}

say() { printf '%s\n' "$*" >&2; }
die() { say "install.sh: $*"; exit 1; }

command -v curl > /dev/null 2>&1 || die "curl is required"
command -v tar > /dev/null 2>&1 || die "tar is required"
if command -v sha256sum > /dev/null 2>&1; then
    sha256() { sha256sum "$1" | cut -d ' ' -f 1; }
elif command -v shasum > /dev/null 2>&1; then
    sha256() { shasum -a 256 "$1" | cut -d ' ' -f 1; }
else
    die "sha256sum or shasum is required"
fi

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) target=x86_64-unknown-linux-musl ;;
    Linux-aarch64 | Linux-arm64) target=aarch64-unknown-linux-musl ;;
    Darwin-arm64) target=aarch64-apple-darwin ;;
    Darwin-x86_64) target=x86_64-apple-darwin ;;
    *) die "no prebuilt burrmill for $(uname -s) $(uname -m); on Windows download the .zip from https://github.com/$repo/releases" ;;
esac

tag=${BURRMILL_VERSION:-}
if [ -z "$tag" ]; then
    # /releases/latest redirects to /releases/tag/<tag>; no API token or JSON parsing needed.
    url=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest") \
        || die "could not find the latest release"
    tag=${url##*/}
fi
case "$tag" in v[0-9]*) ;; *) tag=v$tag ;; esac
case "$tag" in *[!0-9A-Za-z._-]*) die "bad version '$tag'" ;; esac

name=burrmill-$tag-$target
base=https://github.com/$repo/releases/download/$tag
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

say "downloading $name.tar.gz"
curl -fsSL -o "$tmp/$name.tar.gz" "$base/$name.tar.gz" || die "no such release asset: $base/$name.tar.gz"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/SHA256SUMS" || die "could not download SHA256SUMS"

want=$(awk -v f="$name.tar.gz" '$2 == f { print $1 }' "$tmp/SHA256SUMS")
[ -n "$want" ] || die "$name.tar.gz is not in SHA256SUMS"
got=$(sha256 "$tmp/$name.tar.gz")
[ "$want" = "$got" ] || die "checksum mismatch for $name.tar.gz (expected $want, got $got)"

tar -xzf "$tmp/$name.tar.gz" -C "$tmp" "$name/burrmill"
mkdir -p "$dir"
cp "$tmp/$name/burrmill" "$dir/burrmill.new"
chmod 755 "$dir/burrmill.new"
mv -f "$dir/burrmill.new" "$dir/burrmill"

say "installed burrmill $tag to $dir/burrmill"
case ":$PATH:" in
    *":$dir:"*) ;;
    *) say "$dir is not on your PATH" ;;
esac
