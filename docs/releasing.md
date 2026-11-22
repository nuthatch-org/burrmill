# Releasing

A release is a git tag `vX.Y.Z` on main plus a GitHub Release with prebuilt `burrmill` binaries.
Nothing is published to crates.io from the workflow.

1. Add the entry to `CHANGELOG.md`, newest first, heading `## X.Y.Z - YYYY-MM-DD`. Its text is the
   release's notes. The workflow fails without it.
2. Merge it, then tag that commit on main and push the tag:

   ```sh
   git tag vX.Y.Z origin/main && git push origin vX.Y.Z
   ```

3. `.github/workflows/release.yml` does the rest: builds with `--profile cli` for
   `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` (both static, each on a native
   runner), `aarch64-apple-darwin`, `x86_64-apple-darwin` and `x86_64-pc-windows-msvc`; runs a
   strict deterministic receipt and `verify` on each binary (`crates/burrmill-cli/musl-smoke.sh` on
   Linux); packs `burrmill-<tag>-<target>.tar.gz` (`.zip` on Windows) with the binary, both
   licences and the README; writes one `SHA256SUMS`; and creates the Release.

To try the builds without publishing, push `main` to `release-dry-run`
(`git push origin origin/main:refs/heads/release-dry-run --force`): it builds, tests and checksums,
names the archives after the latest CHANGELOG version, and creates nothing. The same can be done by
running the workflow by hand (Actions, release, Run workflow) with a tag name.

`install.sh` at the repo root installs the latest (or `BURRMILL_VERSION`) release's archive for the
current machine into `${BURRMILL_INSTALL_DIR:-$HOME/.local/bin}`, after checking it against
`SHA256SUMS`.

The `burrmill-cli` crate's own `version` is not checked against the tag: it is `publish = false`.
